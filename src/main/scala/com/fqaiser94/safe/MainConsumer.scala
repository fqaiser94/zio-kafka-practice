package com.fqaiser94.safe

import zio.Schedule
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.{ZSink, ZStream}

/**
 * Continuously consumes messages from items topic in kafka and prints it to stdout
 */
object MainConsumer extends zio.App {
  def program(bootstrapServers: List[String]) =
    ZStream
      .repeatEffectWith(
        effect = {
          val settings = ConsumerSettings(bootstrapServers)
            .withGroupId("test-consumer")
            .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Latest))

          Consumer.consumeWith(settings, Subscription.topics("items"), Serde.string, Serde.string) {
            (k, v) => putStr(s"$k:$v")
          }
        },
        schedule = Schedule.spaced(1.seconds))
      .run(ZSink.drain)


  override def run(args: List[String]) = {
    val bootstrapServers = List(args.headOption.getOrElse("kafka:9092"))
    val consumerSettings = ConsumerSettings(bootstrapServers)
    val consumerLayer = Consumer.make(consumerSettings).toLayer

    program(bootstrapServers)
      .provideSomeLayer(Clock.live ++ Blocking.live ++ Console.live ++ consumerLayer)
      .exitCode
  }
}
