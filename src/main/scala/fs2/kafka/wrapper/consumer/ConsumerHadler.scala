package fs2.kafka.wrapper.consumer

import java.util.concurrent.TimeUnit
import java.util.{Collection => JCollection, Map => JMap}

import cats.effect.Async
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration


trait ConsumerHadler[F[_], K, V] {
  def assignment(): F[Set[TopicPartition]]
  def subscription(): F[Set[String]]
  def subscribe(topics: Seq[String]): F[Unit]
  def subscribe(topics: Seq[String], onRevoked: Seq[TopicPartition] => Unit, onAssigned: Seq[TopicPartition] => Unit): F[Unit]
  def unsubscribe(): F[Unit]
  def assign(partition: Seq[TopicPartition]): F[Unit]
  def poll(timeout: Duration): F[Seq[ConsumerRecord[K, V]]]
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
  def close(timeout: Duration): F[Unit]
  def wakeup(): F[Unit]
}


object ConsumerHadler {
  def fromConsumer[F[_], K, V](underline: Consumer[K, V])(implicit F: Async[F]): ConsumerHadler[F, K, V] = {
    new ConsumerHadler[F, K, V] {
      override def assignment() = F.delay {
        underline.assignment().asScala.toSet
      }

      override def subscription() = F.delay {
        underline.subscription().asScala.toSet
      }

      override def subscribe(topics: Seq[String]) = F.delay {
        underline.subscribe(topics.asJava)
      }

      override def subscribe(topics: Seq[String], onRevoked: Seq[TopicPartition] => Unit, onAssigned: Seq[TopicPartition] => Unit) = F.delay {
        underline.subscribe(topics.asJava, new ConsumerRebalanceListener {
          override def onPartitionsRevoked(partitions: JCollection[TopicPartition]) = onRevoked(partitions.asScala.toSeq)

          override def onPartitionsAssigned(partitions: JCollection[TopicPartition]) = onAssigned(partitions.asScala.toSeq)
        })
      }

      override def unsubscribe() = F.delay {
        underline.unsubscribe()
      }

      override def assign(partition: Seq[TopicPartition]) = F.delay {
        underline.assign(partition.asJava)
      }

      override def poll(timeout: Duration) = F.delay {
        underline.poll(timeout.toMillis).asScala.toSeq
      }

      override def commitSync() = F.delay {
        underline.commitSync()
      }

      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]) = F.delay {
        underline.commitSync(offsets.asJava)
      }

      override def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata]) = F.async { r =>
        underline.commitAsync(offsets.asJava, new OffsetCommitCallback {
          override def onComplete(offsets: JMap[TopicPartition, OffsetAndMetadata], exception: Exception) = {
            if (exception == null) {
              r(Left(exception))
            } else {
              r(Right(offsets.asScala.toMap))
            }
          }
        })
      }

      def seek(partition: TopicPartition, offset: Long) = F.delay {
        underline.seek(partition, offset)
      }

      def seekToBeginning(partitions: Seq[TopicPartition]) = F.delay {
        underline.seekToBeginning(partitions.asJava)
      }

      def seekToEnd(partitions: Seq[TopicPartition]) = F.delay {
        underline.seekToEnd(partitions.asJava)
      }

      def position(partition: TopicPartition) = F.delay {
        underline.position(partition)
      }

      def committed(partition: TopicPartition) = F.delay {
        underline.committed(partition)
      }

      def partitionsFor(topic: String) = F.delay {
        underline.partitionsFor(topic).asScala
      }

      def listTopics() = F.delay {
        underline.listTopics().asScala
          .mapValues(_.asScala.toSeq)
          .toMap
      }

      override def pause(partitions: Seq[TopicPartition]) = F.delay {
        underline.pause(partitions.asJava)
      }

      override def resume(partitions: Seq[TopicPartition]) = F.delay {
        underline.resume(partitions.asJava)
      }

      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]) = F.delay {
        underline
          .offsetsForTimes(timestampsToSearch.mapValues(long2Long).asJava)
          .asScala.toMap
      }

      def beginningOffsets(partitions: Seq[TopicPartition]) = F.delay {
        underline
          .beginningOffsets(partitions.asJava)
          .asScala
          .toMap
          .mapValues(Long2long)
      }

      def endOffsets(partitions: Seq[TopicPartition]) = F.delay {
        underline
          .endOffsets(partitions.asJava)
          .asScala
          .toMap
          .mapValues(Long2long)
      }

      override def close() = F.delay {
        underline.close()
      }

      def close(timeout: Duration) = F.delay {
        underline.close(timeout.toMillis, TimeUnit.MILLISECONDS)
      }

      override def wakeup() = F.delay {
        underline.wakeup()
      }
    }
  }
}