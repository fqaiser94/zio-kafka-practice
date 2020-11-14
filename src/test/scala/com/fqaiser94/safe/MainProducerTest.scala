package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.{testConsumerProducerLayer, testProducerLayer}
import zio.Exit.Success
import zio.{Chunk, ZIO}
import zio.blocking.Blocking
import zio.console.putStrLn
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}

object MainProducerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("writes a message to kafka every second") {
      for {
        producerFiber <- MainProducer.program.fork

        bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
        settings = ConsumerSettings(bootstrapServers)
          .withGroupId("test-consumer")
          .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        consumer = Consumer.make(settings)
        msg1Fiber <- consumer.use(_
          .subscribeAnd(Subscription.topics("items"))
          .plainStream(Serde.string, Serde.string)
          .take(1)
          .map(x => (x.key, x.value))
          .runCollect).fork
        _ <- putStrLn("get this far?")
        //_ <- TestClock.adjust(2.seconds)
        //
        _ <- TestClock.adjust(1.seconds)
        _ <- producerFiber.interrupt
        msg <- msg1Fiber.await
        _ <- putStrLn(msg.toString)
       } yield assert(msg)(equalTo(Success(Chunk((null, "0")))))
    }.provideCustomLayer(TestClock.default ++ Kafka.test ++ testConsumerProducerLayer) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
