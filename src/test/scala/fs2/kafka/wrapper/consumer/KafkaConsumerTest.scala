package fs2.kafka.wrapper.consumer


import java.util

import fs2.{Strategy, Task}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, MockConsumer, OffsetResetStrategy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.scalatest.FunSpec

import scala.concurrent.duration._
import scala.collection.JavaConverters._

class KafkaConsumerTest extends FunSpec {

  implicit val S: Strategy = Strategy.fromCachedDaemonPool()

  it("smoke test") {
    val mockConsumer = new MockConsumer[String, String](OffsetResetStrategy.EARLIEST)

    val beginningOffsets = new util.HashMap[TopicPartition, java.lang.Long]
    beginningOffsets.put(new TopicPartition("test", 0), long2Long(0L))
    beginningOffsets.put(new TopicPartition("test", 1), long2Long(0L))
    mockConsumer.updateBeginningOffsets(beginningOffsets)

    val record = new ConsumerRecord("test", 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", "value1")

    mockConsumer.schedulePollTask(() => {
      mockConsumer.rebalance(util.Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1)))
      mockConsumer.addRecord(record)
    })

    val c:Task[Consumer[String, String]] = Task.delay(mockConsumer)
    val s = KafkaConsumer(c).flatMap { client =>
        client.subscribe(Seq("test"), 1.second)
      }

    val records = s.head.runLast.unsafeRun()

    assert(records.map(_.iterator().asScala.toSeq) == Some(Seq(record)))
  }
}
