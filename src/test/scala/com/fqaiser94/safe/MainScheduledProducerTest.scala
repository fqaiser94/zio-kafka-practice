package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.{consumeAllMessagesFromKafka, consumeMessagesFromKafka, kafkaTestTimeout}
import zio.Chunk
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}

object MainScheduledProducerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("writes a message to kafka every second") {
      for {
        _ <- TestClock.setTime(0.seconds)
        _ <- MainScheduledProducer.program.fork
        msg1 <- consumeMessagesFromKafka(1, "items")
        _ <- TestClock.adjust(1.seconds)
        msg2 <- consumeAllMessagesFromKafka("items")
      } yield assert(msg1)(equalTo(Chunk((null, "0")))) && assert(msg2)(equalTo(Chunk((null, "0"), (null, "1"))))
    }.provideSomeLayer(Kafka.test ++ Blocking.live ++ TestClock.default) @@ kafkaTestTimeout
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
