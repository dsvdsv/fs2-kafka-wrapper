package fs2.kafka.wrapper.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer => GenConsumer, KafkaConsumer}

object KafkaConsumer {
  def apply[K, V](consumerSettings: ConsumerSettings[K, V]): GenConsumer[K, V] = {
    val properties = consumerSettings.properties
      .foldLeft(new Properties()) { (p, kv) => p.setProperty(kv._1, kv._2); p }

    new KafkaConsumer[K, V](
      properties,
      consumerSettings.keyDeserializer,
      consumerSettings.valueDeserializer
    )
  }
}
