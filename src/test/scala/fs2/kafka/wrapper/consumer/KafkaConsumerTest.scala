package fs2.kafka.wrapper.consumer

import fs2.{Strategy, Task}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class KafkaConsumerTest extends FunSpec with Matchers {

  implicit val S: Strategy = Strategy.fromCachedDaemonPool()

  it("subscribe and poll") {
    val mockConsumer = mkMockConsumer("test", 2)

    mockConsumer.schedulePollTask(() => {
      genMessages("test", 0).foreach(mockConsumer.addRecord)
    })

    val c: Task[Consumer[String, String]] = Task.delay(mockConsumer)

    val messages =
      KafkaConsumer(c).flatMap { _.subscribe(Seq("test"), 1.second) }
        .take(2) // rebalance + messages
        .runLast.unsafeRun()

    messages.get.count() shouldBe 10
  }

  it("resubscribe should terminate previous subscription") {
    val mockConsumer = mkMockConsumer("test", 1)

    mockConsumer.schedulePollTask(() => {
      genMessages("test", 0).foreach(mockConsumer.addRecord)

      mockConsumer.schedulePollTask(() => {
        genMessages("test", 0).foreach(mockConsumer.addRecord)
      })
    })

    val c: Task[Consumer[String, String]] = Task.delay(mockConsumer)

    val messages = KafkaConsumer(c).flatMap { client =>
      val first = client.subscribe(Seq("test"), 1.second)
      val second = client.subscribe(Seq("test"), 1.second)

      second.take(4)
    }
      .runLast.unsafeRun()

    messages.get.count() shouldBe 10

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
