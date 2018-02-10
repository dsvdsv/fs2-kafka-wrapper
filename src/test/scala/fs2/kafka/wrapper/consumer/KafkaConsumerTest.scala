package fs2.kafka.wrapper.consumer

import cats.effect.IO
import org.apache.kafka.clients.consumer.{ ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class KafkaConsumerTest extends FunSpec with Matchers {

  it("subscribe and poll") {
    val mockConsumer = mkMockConsumer("test", 2)

    mockConsumer.schedulePollTask(() => {
      genMessages("test", 0).foreach(mockConsumer.addRecord)
    })

    val messages =
      Consumer.mkConsumer[IO, String, String](mockConsumer).through(Consumer.subscribeStream(Seq("test"), 1.second))
        .take(10) // rebalance + messages
        .compile.toVector.unsafeRunSync()

    messages.length shouldBe 10
  }

  def genMessages(topic: String, partition: Int) = {
    (0 until 10)
      .map(idx => new ConsumerRecord(topic, partition, idx, s"key$idx", s"value$idx"))
  }

  def mkMockConsumer(topic: String, partitions: Int): MockConsumer[String, String] = {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    mockConsumer.updateBeginningOffsets((0 until partitions)
      .map(idx => new TopicPartition(topic, idx) -> long2Long(idx))
      .toMap.asJava
    )

    mockConsumer.schedulePollTask(() => {
      mockConsumer.rebalance((0 until partitions).map(idx => new TopicPartition(topic, idx)).toList.asJava)
    })

    mockConsumer
  }
}
