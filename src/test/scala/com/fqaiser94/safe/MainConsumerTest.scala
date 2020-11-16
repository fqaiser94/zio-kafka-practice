package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestConsole, TestEnvironment}

object MainConsumerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Should print out messages from items topic") {
      for {
        bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
        producerSettings = ProducerSettings(bootstrapServers)
        producerManaged = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)
        _ <- producerManaged.use(_.produce(new ProducerRecord[String, String]("items", "key1", "value1")))

        _ <- MainConsumer.program.take(1).runDrain
        output1 <- TestConsole.output
      } yield assert(output1)(equalTo(Seq("key1:value1")))
    }.provideSomeLayer(Kafka.test ++ TestConsole.silent ++ Blocking.live ++ Clock.live) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
