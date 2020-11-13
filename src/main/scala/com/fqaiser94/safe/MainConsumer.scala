package com.fqaiser94.safe

import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde

/**
 * Consume messages from items topic in kafka and print it to stdout
 */
object MainConsumer extends zio.App {
  val program: ZIO[Clock with Blocking with Consumer with Console, Throwable, Unit] =
    Consumer.subscribeAnd(Subscription.topics("items"))
      .plainStream(Serde.string, Serde.string)
      .tap(event => putStrLn(s"${event.key}:${event.value}"))
      .runDrain

  override def run(args: List[String]) = {
    val bootstrapServers = List(args.headOption.getOrElse("kafka:9092"))
    val consumerSettings = ConsumerSettings(bootstrapServers)
    val consumerLayer = Consumer.make(consumerSettings).toLayer

    program
      .provideSomeLayer(Clock.live ++ Blocking.live ++ Console.live ++ consumerLayer)
      .exitCode
  }
}
