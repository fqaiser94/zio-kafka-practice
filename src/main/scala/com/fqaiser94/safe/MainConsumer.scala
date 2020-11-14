package com.fqaiser94.safe

import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStr}
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.{ZSink, ZStream}
import zio.{Schedule, ZIO}

/**
 * Consumes messages from items topic and prints them out to stdout every second
 */
object MainConsumer extends zio.App {
  val program: ZIO[Any with Kafka with Console with Blocking with Clock, Nothing, Unit] =
    ZStream
      .repeatEffectWith(
        effect = for {
          bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
          settings = ConsumerSettings(bootstrapServers)
            .withGroupId("test-consumer")
            .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
          _ <- Consumer.consumeWith(settings, Subscription.topics("items"), Serde.string, Serde.string) {
            (k, v) => putStr(s"$k:$v")
          }
        } yield (),
        schedule = Schedule.spaced(1.seconds))
      .run(ZSink.drain)
      .orDie

  override def run(args: List[String]) =
    program
      .provideSomeLayer(Kafka.live ++ Console.live ++ Blocking.live ++ Clock.live)
      .exitCode
}
