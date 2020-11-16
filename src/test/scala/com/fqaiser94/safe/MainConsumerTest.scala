package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.{Chunk, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestConsole, TestEnvironment}
import Utils._

object MainConsumerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Should print out message from items topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1")))

        _ <- MainConsumer.program.takeUntilM(_ => TestConsole.output.map(_.nonEmpty)).runDrain
        output1 <- TestConsole.output
      } yield assert(output1)(equalTo(Seq("key1:value1")))
    } @@ timeout(60.seconds),
    testM("Should print out messages from items topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2")))

        _ <- MainConsumer.program.takeUntilM(_ => TestConsole.output.map(_.size == 2)).runDrain
        output1 <- TestConsole.output
      } yield assert(output1)(equalTo(Seq("key1:value1", "key2:value2")))
    } @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
      .provideSomeLayer(Kafka.test ++ TestConsole.silent ++ Blocking.live ++ Clock.live)

}
