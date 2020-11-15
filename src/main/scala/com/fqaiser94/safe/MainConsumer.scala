package com.fqaiser94.safe

import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStr}
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde

/**
 * Consumes messages from items topic and prints them out to stdout every second
 */
object MainConsumer extends zio.App {
  val program: ZIO[Any with Kafka with Console with Blocking with Clock, Throwable, Unit] = for {
    bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
    settings = ConsumerSettings(bootstrapServers)
      .withGroupId("consumer")
      .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
    _ <- Consumer.consumeWith(settings, Subscription.topics("items"), Serde.string, Serde.string) {
      (k, v) => putStr(s"$k:$v")
    }
  } yield ()

  override def run(args: List[String]) =
    program
      .provideSomeLayer(Kafka.live ++ Console.live ++ Blocking.live ++ Clock.live)
      .exitCode
}
