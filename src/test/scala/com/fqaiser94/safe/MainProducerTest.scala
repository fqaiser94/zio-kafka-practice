package com.fqaiser94.safe

import com.fqaiser94.safe.Utils.consumeMessagesFromKafka
import zio.Chunk
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}

object MainProducerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("writes a message to kafka") {
      for {
        _ <- TestClock.setTime(0.second)
        _ <- MainProducer.program
        msg1 <- consumeMessagesFromKafka(1, "items")
      } yield assert(msg1)(equalTo(Chunk((null, "0"))))
    }.provideSomeLayer(Kafka.test ++ Blocking.live ++ TestClock.default) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
