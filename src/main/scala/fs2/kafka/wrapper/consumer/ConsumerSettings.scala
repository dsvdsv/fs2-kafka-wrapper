package fs2.kafka.wrapper.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer

case class ConsumerSettings[K, V](properties: Map[String, String] = Map.empty)
  (implicit val keyDeserializer: Deserializer[K], val valueDeserializer: Deserializer[V]) {
  def withBootstrapServers(bootstrapServers: String) = withProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  def withAutoCommit(autoCommit: Boolean) = withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString)
  def withGroupId(groupId: String) = withProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  def withMaxPollRecords(maxPollRecords: Long) = withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
  def withAutoOffsetReset(reset: String) = withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset)
  def withClientId(clientId: String) = withProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)

  def withProperty(key: String, value: String) = copy[K, V](properties = properties.updated(key, value))

}
