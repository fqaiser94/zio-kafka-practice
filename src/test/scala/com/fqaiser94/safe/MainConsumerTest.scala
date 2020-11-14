package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.testConsumerProducerLayer
import org.apache.kafka.clients.producer.ProducerRecord
import zio.ZIO
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.kafka.producer.Producer
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestClock, TestConsole, TestEnvironment}

object MainConsumerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Consumes all messages from kafka topic and prints them to console") {
      val expected = Seq("key1:value1", "key2:value2")
      for {
        bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
        programFiber <- MainConsumer.program(bootstrapServers).fork
        _ <- Producer.produce[Any, String, String](new ProducerRecord[String, String]("items", "key1", "value1"))
        _ <- TestClock.adjust(1.seconds)
        _ <- Producer.produce[Any, String, String](new ProducerRecord[String, String]("items", "key1", "value1"))
        _ <- TestClock.adjust(2.seconds)
        output <- TestConsole.output
        _ <- programFiber.interrupt
      } yield assert(output.toSeq)(equalTo(expected))
    }.provideCustomLayer(TestConsole.silent ++ Kafka.test ++ testConsumerProducerLayer) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
