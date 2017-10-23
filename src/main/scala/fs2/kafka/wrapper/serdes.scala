package fs2.kafka.wrapper

import java.util.{Map => JMap}

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._

trait DefaultSerializers {
  implicit val byteArraySerializer: Serializer[Array[Byte]] = new ByteArraySerializer()
  implicit val stringSerializer: Serializer[String] = new StringSerializer()
  implicit val longSerializer: Serializer[Long] = wrap(new LongSerializer(), Long.box)
  implicit val intSerializer: Serializer[Int] = wrap(new IntegerSerializer, Int.box)
  implicit val doubleSerializer: Serializer[Double] = wrap(new DoubleSerializer, Double.box)

  private def wrap[T, U](underlying: Serializer[U], fn: T => U) = new Serializer[T] {
    override def serialize(topic: String, data: T) =
      underlying.serialize(topic, fn(data))

    override def close() =
      underlying.close()

    override def configure(configs: JMap[String, _], isKey: Boolean) =
      underlying.configure(configs, isKey)
  }
}

trait DefaultDeserializers {
  implicit val byteArrayDeserializer: Deserializer[Array[Byte]] = new ByteArrayDeserializer()
  implicit val stringDeserializer: Deserializer[String] = new StringDeserializer()
  implicit val longDeserializer: Deserializer[Long] = wrap(new LongDeserializer(), Long.unbox)
  implicit val intDeserializer: Deserializer[Int] = wrap(new IntegerDeserializer, Int.unbox)
  implicit val doubleDeserializer: Deserializer[Double] = wrap(new DoubleDeserializer, Double.unbox)

  private def wrap[T, U](underlying: Deserializer[U], fn: U => T) = new Deserializer[T] {
    override def configure(configs: JMap[String, _], isKey: Boolean) =
      underlying.configure(configs, isKey)

    override def close() =
      underlying.close()

    override def deserialize(topic: String, data: Array[Byte]) =
      underlying.deserialize(topic, data) match {
        case null => throw new SerializationException(s"Value on $topic is null")
        case v    => fn(v)
      }
  }
}

trait DefaultSerialization extends DefaultSerializers with DefaultDeserializers

object DefaultSerialization extends DefaultSerialization