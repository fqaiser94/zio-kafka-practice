package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.stream.ZStream
import zio.{Schedule, ZIO}

/**
 * Produces a message to items topic every second
 */
object MainScheduledProducer extends zio.App {
  val program: ZIO[Any with Blocking with Clock with Kafka, Throwable, Unit] =
    ZStream
      .repeatEffectWith(
        effect = for {
          time <- ZIO.accessM[Clock](_.get.currentDateTime).orDie
          bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
          producerSettings = ProducerSettings(bootstrapServers)
          producerManaged = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)
          record = new ProducerRecord("items", null.asInstanceOf[String], time.toEpochSecond.toString)
          r <- producerManaged.use(_.produce(record))
        } yield (),
        schedule = Schedule.spaced(1.seconds))
      .runDrain

  override def run(args: List[String]) =
    program
      .provideSomeLayer(Kafka.live ++ Blocking.live ++ Clock.live ++ Console.live)
      .exitCode
}
