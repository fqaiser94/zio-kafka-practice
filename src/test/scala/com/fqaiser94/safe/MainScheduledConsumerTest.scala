package com.fqaiser94.safe

import org.apache.kafka.clients.producer.ProducerRecord
import zio.ZIO
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestClock, TestConsole, TestEnvironment}

object MainScheduledConsumerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Should consume and print out messages from items topic in Kafka every second") {
      for {
        _ <- TestClock.setTime(0.seconds)
        _ <- MainScheduledConsumer.program.fork

        bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
        producerSettings = ProducerSettings(bootstrapServers)
        producerManaged = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)

        _ <- producerManaged.use(_.produce(new ProducerRecord[String, String]("items", "key1", "value1")))
        _ <- TestClock.setTime(1.seconds)
        output1 <- TestConsole.output

        _ <- producerManaged.use(_.produce(new ProducerRecord[String, String]("items", "key2", "value2")))
        _ <- TestClock.setTime(2.seconds)
        output2 <- TestConsole.output

      } yield assert(output1)(equalTo(Seq("key1:value1"))) && assert(output2)(equalTo(Seq("key1:value1", "key2:value2")))
    } @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
      .provideSomeLayer(Kafka.test ++ TestConsole.silent ++ Blocking.live ++ TestClock.default)

}
