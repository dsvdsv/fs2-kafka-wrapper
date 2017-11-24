package fs2.kafka.wrapper.consumer

import java.util.Properties

import fs2._
import fs2.async.mutable.Signal
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp, KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

trait KafkaConsumer[F[_], K, V] {
  def subscribe(topics: Seq[String], timeout: FiniteDuration): Stream[F, ConsumerRecords[K, V]]
  def assign(partition: Seq[TopicPartition], timeout: FiniteDuration): Stream[F, ConsumerRecords[K, V]]
  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Map[TopicPartition, OffsetAndMetadata]]
  def seek(topicPartition: TopicPartition, offset: Long): F[Unit]
  def seekToBeginning(partitions: Seq[TopicPartition]): F[Unit]
  def seekToEnd(partitions: Seq[TopicPartition]): F[Unit]
  def position(partition: TopicPartition): F[Long]
  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]]
  def beginningOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]]
  def endOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]]
}

object KafkaConsumer {

  def apply[F[_], K, V](consumerSettings: ConsumerSettings[K, V])
    (implicit F: Async[F]): Stream[F, KafkaConsumer[F, K, V]] = {
    val c: F[Consumer[K, V]] = F.delay {
      val properties = consumerSettings.properties
        .foldLeft(new Properties()) { (p, kv) => p.setProperty(kv._1, kv._2); p }

      new JKafkaConsumer[K, V](
        properties,
        consumerSettings.keyDeserializer,
        consumerSettings.valueDeserializer
      )
    }
    apply(c)
  }

  def apply[F[_], K, V](c: F[Consumer[K, V]])
    (implicit F: Async[F]): Stream[F, KafkaConsumer[F, K, V]] = {
    val handler = c.map(ConsumerHandler.fromConsumer[F, K, V])

    final case class SubscriptionState(cleanup: () => F[Unit])

    val consumer: F[KafkaConsumer[F, K, V]] =
      F.refOf[Option[SubscriptionState]](None).map { termSigRef =>
        def poll(subscribe: () => F[Unit], timeout: FiniteDuration): Stream[F, ConsumerRecords[K, V]] = for {
          termSignal <- Stream.eval(async.signalOf[F, Boolean](false))
          _ <- Stream.eval(registerTermSign(termSignal))
          _ <- Stream.eval(subscribe())
          poll <- Stream.repeatEval(handler.flatMap(_.poll(timeout))).interruptWhen(termSignal.discrete)
        } yield poll

        def registerTermSign(termSignal: Signal[F, Boolean]): F[Unit] = for {
          change <- termSigRef.modify { _ => Some(SubscriptionState(() => termSignal.set(true))) }
          _ <- change.previous.toSeq.traverse(_.cleanup())
        } yield ()

        new KafkaConsumer[F, K, V] {
          override def subscribe(topics: Seq[String], timeout: FiniteDuration) =
            poll(() => handler.flatMap(_.subscribe(topics)), timeout)

          override def assign(partitions: Seq[TopicPartition], timeout: FiniteDuration) =
            poll(() => handler.flatMap(_.assign(partitions)), timeout)

          override def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = handler.flatMap(_.commitAsync(offsets))

          override def seek(topicPartition: TopicPartition, offset: Long) = handler.flatMap(_.seek(topicPartition, offset))

          override def seekToBeginning(partitions: Seq[TopicPartition]) = handler.flatMap(_.seekToBeginning(partitions))

          override def seekToEnd(partitions: Seq[TopicPartition]) = handler.flatMap(_.seekToEnd(partitions))

          override def position(partition: TopicPartition) = handler.flatMap(_.position(partition))

          override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = handler.flatMap(_.offsetsForTimes(timestampsToSearch))

          override def beginningOffsets(partitions: Seq[TopicPartition]) = handler.flatMap(_.beginningOffsets(partitions))

          override def endOffsets(partitions: Seq[TopicPartition]) = handler.flatMap(_.endOffsets(partitions))
        }
      }

    Stream.bracket(consumer)(Stream.emit, (_) => handler.flatMap(_.close()))
  }

}
