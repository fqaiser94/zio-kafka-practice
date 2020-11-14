package com.fqaiser94.safe

import zio.console.putStr
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.{ZSink, ZStream}
import zio.{Schedule, ZIO}

/**
 * Continuously consumes messages from items topic in kafka and prints it to stdout
 */
object MainConsumer { // extends zio.App {
  val program =
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

  //  override def run(args: List[String]) = {
  //    val bootstrapServers = List(args.headOption.getOrElse("kafka:9092"))
  //    val consumerSettings = ConsumerSettings(bootstrapServers)
  //    val consumerLayer = Consumer.make(consumerSettings).toLayer
  //
  //    program
  //      .provideSomeLayer(Clock.live ++ Blocking.live ++ Console.live ++ consumerLayer)
  //      .exitCode
  //  }
}
