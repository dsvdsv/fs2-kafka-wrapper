package fs2.kafka.wrapper.consumer

import fs2.util.Async
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition


trait ConsumerHandler[F[_], K, V] {
  def assignment(): F[Set[TopicPartition]]
  def subscription(): F[Set[String]]
  def subscribe(topics :Seq[String]): F[Unit]
  def unsubscribe(): F[Unit]
  def assign(partition: Seq[TopicPartition]): F[Unit]
  def poll: F[ConsumerRecords[K, V]]
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]):F[Unit]
  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]):F[Unit]
  def pause(partitions: Seq[TopicPartition]): F[Unit]
  def resume(partitions: Seq[TopicPartition]): F[Unit]
  def close: F[Unit]
  def wakeup: F[Unit]
}


object ConsumerHandler {
  def fromConsumer[F[_], K, V](consumer: Consumer[K, V])(implicit F: Async[F]): ConsumerHandler[F, K, V] = {
    new ConsumerHandler[F, K, V] {override def assignment() = ???

      override def subscription() = F.delay{ consumer.subscription() }

      override def subscribe(topics: Seq[String]) = F.delay{ consumer.subscribe(topics)}

      override def unsubscribe() = ???

      override def assign(partition: Seq[TopicPartition]) = ???

      override def poll = ???

      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]) = ???

      override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]) = ???

      override def pause(partitions: Seq[TopicPartition]) = ???

      override def resume(partitions: Seq[TopicPartition]) = ???

      override def close = F.delay{ consumer.close() }

      override def wakeup = F.delay{ consumer.wakeup() }
    }
  }
}