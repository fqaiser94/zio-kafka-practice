package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.{ZSink, ZStream}
import zio.{Schedule, ZIO}

/**
 * Continuously produces a message to items topic in kafka every second
 */
object MainProducer extends zio.App {
  val program: ZIO[Any with Blocking with Producer[Any, String, String] with Clock, Throwable, Unit] =
    ZStream
      .repeatEffectWith(
        effect = for {
          time <- ZIO.accessM[Clock](_.get.currentDateTime).orDie
          record = new ProducerRecord("items", null.asInstanceOf[String], time.toInstant.toEpochMilli.toString)
        } yield record,
        schedule = Schedule.spaced(1.seconds))
      .run(ZSink.foreach(record => Producer.produceAsync[Any, String, String](record)))

  override def run(args: List[String]) = {
    val bootstrapServers = List(args.headOption.getOrElse("kafka:9092"))
    val producerSettings = ProducerSettings(bootstrapServers)
    val producerLayer = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string).toLayer

    program
      .provideSomeLayer(producerLayer ++ Blocking.live ++ Clock.live)
      .exitCode
  }
}
