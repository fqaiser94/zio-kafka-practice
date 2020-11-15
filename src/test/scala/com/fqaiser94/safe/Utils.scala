package com.fqaiser94.safe

import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.{Chunk, ZIO}

object Utils {

  /**
   * Blocks until numMessages have been consumed
   */
  val consumeMessagesFromKafka: (Int, String) => ZIO[Kafka, Throwable, Chunk[(String, String)]] =
    (numMessages: Int, topic: String) => for {
      bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
      settings = ConsumerSettings(bootstrapServers)
        .withGroupId("test-consumer")
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      messages <- Consumer.make(settings).use(_
        .subscribeAnd(Subscription.topics(topic))
        .plainStream(Serde.string, Serde.string)
        .take(numMessages)
        .map(x => (x.key, x.value))
        .runCollect).provideSomeLayer(Clock.live ++ Blocking.live)
    } yield messages

}
