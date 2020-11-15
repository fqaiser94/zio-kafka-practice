package com.fqaiser94.safe

import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStr}
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.ZStream

/**
 * Consumes messages from items topic and prints them out to stdout every second
 */
object MainConsumer { //extends zio.App {
  val program = for {
    bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
    settings = ConsumerSettings(bootstrapServers)
      .withGroupId("consumer")
      .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
    consumer = Consumer.make(settings)
    stream = Consumer
      .subscribeAnd(Subscription.topics("items"))
      .plainStream(Serde.string, Serde.string)
      .tap(cr => putStr(s"${cr.record.key}:${cr.record.value}"))
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapM(_.commit)
  } yield stream

//  override def run(args: List[String]) =
//    program.flatMap(_.runDrain)
//      .provideSomeLayer(Kafka.live ++ Console.live ++ Blocking.live ++ Clock.live)
//      .exitCode
}
