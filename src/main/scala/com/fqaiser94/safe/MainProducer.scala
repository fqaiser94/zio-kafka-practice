package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde

/**
 * Produces a message to items topic every second
 */
object MainProducer extends zio.App {
  val program: ZIO[Any with Blocking with Clock with Kafka, Throwable, Unit] = for {
    time <- ZIO.accessM[Clock](_.get.currentDateTime).orDie
    bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
    producerSettings = ProducerSettings(bootstrapServers)
    producerManaged = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)
    record = new ProducerRecord("items", null.asInstanceOf[String], time.toEpochSecond.toString)
    _ <- producerManaged.use(_.produce(record))
  } yield ()

  override def run(args: List[String]) =
    program
      .provideSomeLayer(Kafka.live ++ Blocking.live ++ Clock.live ++ Console.live)
      .exitCode
}
