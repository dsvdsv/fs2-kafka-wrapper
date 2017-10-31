package fs2.kafka.wrapper

import fs2.kafka.wrapper.consumer.{KafkaConsumer}
import fs2._

class kafka {

  def consumer[F[_], K, V](): Stream[F, KafkaConsumer[F, K, V]] = ???

  //  def producer[F[_], K, V]():Stream[F, ProducerHandler[F, K, V]] = ???

}
