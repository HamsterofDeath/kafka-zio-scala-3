import zio.*
import zio.json.*
import zio.kafka.consumer.*
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.*
import zio.stream.ZStream

import java.time.ZonedDateTime
import java.util
import scala.util

object MainApp extends ZIOAppDefault {


  given encEvent: JsonEncoder[JsonEvent] = {
    given encEntry: JsonEncoder[JsonEntry] = DeriveJsonEncoder.gen

    DeriveJsonEncoder.gen
  }

  given decEvent: JsonDecoder[JsonEvent] = {
    given decEntry: JsonDecoder[JsonEntry] = DeriveJsonDecoder.gen

    DeriveJsonDecoder.gen
  }

  case class JsonEntry(data: Array[Byte])

  case class JsonEvent(
                        name: String,
                        id: Long,
                        when: ZonedDateTime,
                        entryList: List[JsonEntry],
                        entryMap: Map[String, JsonEntry]
                      )


  private val eventSerde = {
    Serde.string.inmapM { s =>
      ZIO.fromEither(s.fromJson[JsonEvent].left.map(str => throw new RuntimeException(str)))
    } { event =>
      ZIO.attempt(event.toJson)
    }
  }

  val producer: ZStream[Producer, Throwable, Nothing] = {
    val rnd = new scala.util.Random()
    ZStream
      .fromIterator {
        Iterator.continually {
          JsonEvent(
            s"Event ${rnd.alphanumeric.take(10).mkString}",
            rnd.nextLong(1000),
            ZonedDateTime.now(),
            (0 to rnd.nextInt(5)).map { _ =>
              JsonEntry(rnd.nextBytes(rnd.nextInt(64)))
            }.toList,
            (0 to rnd.nextInt(5)).map { i =>
              i.toString -> JsonEntry(rnd.nextBytes(rnd.nextInt(64)))
            }.toMap
          )
        }
      }.schedule(Schedule.fixed(2.seconds))
      .mapZIO { event =>
        Producer.produce[Any,Long,JsonEvent](
          topic = "random",
          key = 1337L,
          value = event,
          keySerializer = Serde.long,
          valueSerializer = eventSerde
        )
      }
      .drain
  }

  val consumer: ZStream[Consumer, Throwable, Nothing] =
    Consumer
      .plainStream(Subscription.topics("random"), Serde.long, eventSerde)
      .tap(r => Console.printLine(r.value))
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .drain

  def producerLayer =
    ZLayer.scoped(
      Producer.make(
        settings = ProducerSettings(List("localhost:29092"))
      )
    )

  def consumerLayer =
    ZLayer.scoped(
      Consumer.make(
        ConsumerSettings(List("localhost:29092")).withGroupId("group")
      )
    )

  override def run =
    producer.merge(consumer)
      .runDrain
      .provide(producerLayer, consumerLayer)
}