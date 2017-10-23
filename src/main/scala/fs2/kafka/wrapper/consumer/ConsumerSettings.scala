package fs2.kafka.wrapper.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.duration.FiniteDuration

case class ConsumerSettings[K, V](pollInterval: FiniteDuration, properties: Map[String, String] = Map.empty)
  (implicit val keyDeserializer: Deserializer[K], val valueDeserializer: Deserializer[V]) {
  def withBootstrapServers(bootstrapServers: String) = withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  def withAutoCommit(autoCommit: Boolean) = withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)
  def withGroupId(groupId: String) = withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  def withPollInterval(interval: FiniteDuration) = copy[K, V](pollInterval = interval)
  def withMaxPollRecords(maxPollRecords: Long) = withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
  def withAutoOffsetReset(reset: String) = withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset)
  def withClientId(clientId: String) = withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  def withProperty(key: String, value: String) = copy[K, V](properties = properties.updated(key, value))

}

sealed trait Subscription
sealed trait AutoSubscription extends Subscription
sealed trait ManualSubscription extends Subscription

object Subscriptions {

  private[wrapper] final case class TopicsSubscription(topics: Set[String]) extends AutoSubscription
  private[wrapper] final case class TopicsPatternSubscription(pattern: String) extends AutoSubscription
  private[wrapper] final case class ManualAssignment(assignments: Set[TopicPartition]) extends ManualSubscription
  private[wrapper] final case class ManualAssignmentWithOffsets(assignmentsAndOffsets: Map[TopicPartition, Long]) extends ManualSubscription

  def topics(topics: Set[String]) = TopicsSubscription(topics)
  def topics(topics: String*) = TopicsSubscription(topics.toSet)
  def topicPattern(pattern: String) = TopicsPatternSubscription(pattern)
  def assignment(topicPartitions: Set[TopicPartition]) = ManualAssignment(topicPartitions)
  def assignmentWithOffsets(partitionsAndOffsets: Map[TopicPartition, Long]) = ManualAssignmentWithOffsets(partitionsAndOffsets)
}
