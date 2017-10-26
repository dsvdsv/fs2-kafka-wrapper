package fs2.kafka.wrapper.consumer

import java.util.concurrent.TimeUnit
import java.util.{Collection => JCollection, Map => JMap}

import fs2.util.Async
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._


trait ConsumerHandler[F[_], K, V] {
  def assignment(): F[Set[TopicPartition]]
  def subscription(): F[Set[String]]
  def subscribe(topics: Seq[String])
  def subscribe(topics: Seq[String], onRevoked: Seq[TopicPartition] => Unit, onAssigned: Seq[TopicPartition] => Unit): F[Unit]
  def unsubscribe(): F[Unit]
  def assign(partition: Seq[TopicPartition]): F[Unit]
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]
  def commitSync(): F[Unit]
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Map[TopicPartition, OffsetAndMetadata]]
  def seek(topicPartition: TopicPartition, offset: Long): F[Unit]
  def seekToBeginning(partitions: Seq[TopicPartition]): F[Unit]
  def seekToEnd(partitions: Seq[TopicPartition]): F[Unit]
  def position(partition: TopicPartition): F[Long]
  def committed(partition: TopicPartition): F[OffsetAndMetadata]
  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def listTopics(): F[Map[String, Seq[PartitionInfo]]]
  def pause(partitions: Seq[TopicPartition]): F[Unit]
  def resume(partitions: Seq[TopicPartition]): F[Unit]
  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]]
  def beginningOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]]
  def endOffsets(partitions: Seq[TopicPartition]): F[Map[TopicPartition, Long]]
  def close(): F[Unit]
  def close(timeout: FiniteDuration): F[Unit]
  def wakeup(): F[Unit]
}


object ConsumerHandler {
  def fromConsumer[F[_], K, V](consumer: Consumer[K, V])(implicit F: Async[F]): ConsumerHandler[F, K, V] = {
    new ConsumerHandler[F, K, V] {
      override def assignment() = F.delay {
        consumer.assignment().asScala.toSet
      }

      override def subscription() = F.delay {
        consumer.subscription().asScala.toSet
      }

      override def subscribe(topics: Seq[String]) = F.delay {
        consumer.subscribe(topics.asJava)
      }

      override def subscribe(topics: Seq[String], onRevoked: Seq[TopicPartition] => Unit, onAssigned: Seq[TopicPartition] => Unit) = F.delay {
        consumer.subscribe(topics.asJava, new ConsumerRebalanceListener {
          override def onPartitionsRevoked(partitions: JCollection[TopicPartition]) = onRevoked(partitions.asScala)

          override def onPartitionsAssigned(partitions: JCollection[TopicPartition]) = onAssigned(partitions.asScala)
        })
      }

      override def unsubscribe() = F.delay {
        consumer.unsubscribe()
      }

      override def assign(partition: Seq[TopicPartition]) = F.delay {
        consumer.assign(partition.asJava)
      }

      override def poll(timeout: FiniteDuration) = F.delay {
        consumer.poll(timeout.toMillis)
      }

      override def commitSync() = F.delay {
        consumer.commitSync()
      }

      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]) = F.delay {
        consumer.commitSync(offsets.asJava)
      }

      override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]) = F.async { r =>
        F.delay {
          consumer.commitAsync(offsets.asJava, new OffsetCommitCallback {
            override def onComplete(offsets: JMap[TopicPartition, OffsetAndMetadata], exception: Exception) = {
              if (exception == null) {
                r(Left(exception))
              } else {
                r(Right(offsets.asScala.toMap))
              }
            }
          })
        }
      }

      def seek(partition: TopicPartition, offset: Long) = F.delay {
        consumer.seek(partition, offset)
      }

      def seekToBeginning(partitions: Seq[TopicPartition]) = F.delay {
        consumer.seekToBeginning(partitions.asJava)
      }

      def seekToEnd(partitions: Seq[TopicPartition]) = F.delay {
        consumer.seekToEnd(partitions.asJava)
      }

      def position(partition: TopicPartition) = F.delay {
        consumer.position(partition)
      }

      def committed(partition: TopicPartition) = F.delay {
        consumer.committed(partition)
      }

      def partitionsFor(topic: String) = F.delay {
        consumer.partitionsFor(topic).asScala
      }

      def listTopics() = F.delay {
        consumer.listTopics().asScala
          .mapValues(_.asScala.toSeq)
          .toMap
      }

      override def pause(partitions: Seq[TopicPartition]) = F.delay {
        consumer.pause(partitions.asJava)
      }

      override def resume(partitions: Seq[TopicPartition]) = F.delay {
        consumer.resume(partitions.asJava)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = F.delay {
        consumer
          .offsetsForTimes(timestampsToSearch.mapValues(long2Long).asJava)
          .asScala.toMap
      }

      def beginningOffsets(partitions: Seq[TopicPartition]) = F.delay {
        consumer
          .beginningOffsets(partitions.asJava)
          .asScala
          .toMap
          .mapValues(Long2long)
      }

      def endOffsets(partitions: Seq[TopicPartition]) = F.delay {
        consumer
          .endOffsets(partitions.asJava)
          .asScala
          .toMap
          .mapValues(Long2long)
      }

      override def close() = F.delay {
        consumer.close()
      }

      def close(timeout: FiniteDuration) = F.delay {
        consumer.close(timeout.toMillis, TimeUnit.MILLISECONDS)
      }

      override def wakeup() = F.delay {
        consumer.wakeup()
      }
    }
  }
}