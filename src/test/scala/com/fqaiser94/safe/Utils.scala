package com.fqaiser94.safe

import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.TestFailure
import zio.{Has, ZLayer}

object Utils {

  val testConsumerLayer: ZLayer[Clock with Blocking with Has[Kafka.Service], TestFailure[Throwable], Has[Consumer.Service]] =
    ZLayer.fromServiceManaged((kafka: Kafka.Service) => {
      val consumerSettings = ConsumerSettings(kafka.bootstrapServers)
        .withGroupId("test-consumer")
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))

      Consumer.make(settings = consumerSettings)
    }).mapError(TestFailure.fail)

  val testProducerLayer: ZLayer[Any with Has[Kafka.Service], TestFailure[Throwable], Has[Producer.Service[Any, String, String]]] =
    ZLayer.fromServiceManaged((kafka: Kafka.Service) => {
      val producerSettings = ProducerSettings(kafka.bootstrapServers)
      Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)
    }).mapError(TestFailure.fail)

  val testConsumerProducerLayer: ZLayer[Any, TestFailure[Throwable], Has[Consumer.Service] with Has[Producer.Service[Any, String, String]]] = {
    val testEnv = Blocking.live ++ Clock.live ++ Kafka.test
    testEnv >>> (testConsumerLayer ++ testProducerLayer)
  }

}
