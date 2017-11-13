package fs2.kafka.wrapper.consumer

import java.util.Properties

import fs2._
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp, KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._

trait KafkaConsumer[F[_], K, V] {

  def subscribe(topics: Seq[String], timeout: FiniteDuration): Stream[F,  ConsumerRecords[K, V]]

  def assign(partition: Seq[TopicPartition], timeout: FiniteDuration): Stream[F,  ConsumerRecords[K, V]]

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

    val consumer:F[KafkaConsumer[F, K, V]] = async.signalOf[F, Boolean](false).map { termSignal =>
      new KafkaConsumer[F, K, V] {
        override def subscribe(topics: Seq[String], timeout: FiniteDuration) = for {
          _ <-  Stream.eval(handler.flatMap(_.subscribe(topics)) >> termSignal.modify(!_))
          p <-  Stream.repeatEval(handler.flatMap(_.poll(timeout)))
        } yield p

        override def assign(partitions: Seq[TopicPartition], timeout: FiniteDuration) = for {
          _ <-  Stream.eval(handler.flatMap(_.assign(partitions)) >> termSignal.modify(!_))
          p <-  Stream.repeatEval(handler.flatMap(_.poll(timeout))).interruptWhen(termSignal.discrete)
        } yield p

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
