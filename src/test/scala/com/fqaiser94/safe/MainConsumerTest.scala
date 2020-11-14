package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.testConsumerProducerLayer
import org.apache.kafka.clients.producer.ProducerRecord
import zio.duration.durationInt
import zio.kafka.producer.Producer
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{flaky, timeout}
import zio.test._
import zio.test.environment.{TestClock, TestConsole, TestEnvironment}

object MainConsumerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Should consume and print out messages from items topic in Kafka every second") {
      for {
        programFiber <- MainConsumer.program.fork
        _ <- Producer.produce[Any, String, String](new ProducerRecord[String, String]("items", "key1", "value1"))
        _ <- TestClock.adjust(1.seconds)
        output1 <- TestConsole.output
        _ <- TestConsole.clearOutput
        _ <- Producer.produce[Any, String, String](new ProducerRecord[String, String]("items", "key2", "value2"))
        _ <- TestClock.adjust(2.seconds)
        output2 <- TestConsole.output
        _ <- TestConsole.clearOutput
        _ <- programFiber.interrupt
      } yield assert(output1)(equalTo(Seq("key1:value1"))) &&
        assert(output2)(equalTo(Seq("key2:value2")))
    }.provideCustomLayer(TestConsole.silent ++ Kafka.test ++ testConsumerProducerLayer) @@ flaky @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
