package fs2.kafka.wrapper.consumer

import java.util.Properties

import fs2._
import fs2.Pull
import fs2.util.Async
import fs2.util.syntax._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer => JKafkaConsumer}


trait KafkaConsumer[F[_], K, V] {

  def subscribe(topic: String)(h: ConsumerHandler[F, K, V]): Pull[F, ConsumerRecords[K, V], Unit] = {
    for {
      r <- Pull.eval(h.poll(1.second))
    }
  }

}

object KafkaConsumer {

  def apply[F[_], K, V](consumerSettings: ConsumerSettings[K, V])(implicit F: Async[F]): Pull[F, Nothing, ConsumerHandler[F, K, V]] = {
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

  def apply[F[_] : Async, K, V](c: F[Consumer[K, V]]): Pull[F, Nothing, ConsumerHandler[F, K, V]] =
    Pull.acquire(c.map(ConsumerHandler.fromConsumer))(_.close())

}
