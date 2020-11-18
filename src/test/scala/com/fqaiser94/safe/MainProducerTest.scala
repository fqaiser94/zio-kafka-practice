package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.{consumeAllMessagesFromKafka, consumeMessagesFromKafka, kafkaTestTimeout}
import zio.Chunk
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}

object MainProducerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("writes a message to kafka") {
      val expectedMsg = Chunk((null, "0"))
      for {
        _ <- TestClock.setTime(0.second)
        _ <- MainProducer.program
        msg <- consumeMessagesFromKafka(1, "items")
        allMsgs <- consumeAllMessagesFromKafka("items")
      } yield assert(msg)(equalTo(expectedMsg)) && assert(allMsgs)(equalTo(expectedMsg))
    }.provideSomeLayer(Kafka.test ++ Blocking.live ++ TestClock.default) @@ kafkaTestTimeout
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
