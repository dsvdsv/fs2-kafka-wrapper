package fs2.kafka.wrapper.consumer

import java.util.Properties

import cats.effect.Async
import fs2._
import org.apache.kafka.clients.consumer.{Consumer => JConsumer, ConsumerRecord, KafkaConsumer => JKafkaConsumer}

import scala.concurrent.duration._

object Consumer {

  def mkConsumer[F[_], K, V](consumerSettings: ConsumerSettings[K, V])
    (implicit F: Async[F]): Stream[F, ConsumerHadler[F, K, V]] = {
    val jc: F[JConsumer[K, V]] = F.delay {
      val properties = consumerSettings.properties
        .foldLeft(new Properties()) { (p, kv) => p.setProperty(kv._1, kv._2); p }

      new JKafkaConsumer[K, V](
        properties,
        consumerSettings.keyDeserializer,
        consumerSettings.valueDeserializer
      )
    }
    Stream.eval(jc).flatMap(mkConsumer(_))
  }

  def mkConsumer[F[_], K, V](jc: JConsumer[K, V])
    (implicit F: Async[F]): Stream[F, ConsumerHadler[F, K, V]] = {

    val consumer = F.delay(ConsumerHadler.fromConsumer(jc))
    Stream.bracket(consumer)(Stream.emit(_), (_) => F.flatMap(consumer)(_.close()))
  }

  def subscribeStream[F[_], K, V](topics: Seq[String], timeout: Duration): Pipe[F, ConsumerHadler[F, K, V], ConsumerRecord[K, V]] = {
    _.flatMap { ch =>
      for {
        _       <- Stream.eval(ch.subscribe(topics))
        records <- Stream.repeatEval(ch.poll(timeout))
        poll    <- Stream.emits(records)
      } yield poll
    }
  }
}
