package com.fqaiser94.safe

import zio.ZLayer
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
  val program = {
    val consumerLayer =
      ZLayer.fromServiceManaged((kafka: Kafka.Service) => {
        val consumerSettings = ConsumerSettings(kafka.bootstrapServers)
          .withGroupId("consumer")
          .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        Consumer.make(consumerSettings)
      })

    val stream = Consumer
      .subscribeAnd(Subscription.topics("items"))
      .plainStream(Serde.string, Serde.string)
      .tap(cr => putStr(s"${cr.record.key}:${cr.record.value}"))
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapM(_.commit)

    stream.provideSomeLayer[Kafka with Blocking with Clock with Console](consumerLayer)
  }

  override def run(args: List[String]) =
    program.runDrain
      .provideSomeLayer(Kafka.live ++ Console.live ++ Blocking.live ++ Clock.live)
      .exitCode
}
