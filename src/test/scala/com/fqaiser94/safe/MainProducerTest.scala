package com.fqaiser94.safe

import zio.duration.{Duration, durationInt}
import zio.kafka.consumer.{Consumer, Subscription}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}
import com.fqaiser94.safe.Utils.testConsumerProducerLayer

object MainProducerTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("writes a message to kafka every second") {
      val expectedMessages = (0 to 4).map(num => (null, (num * 1000).toString))
      for {
        producerFiber <- MainProducer.program.fork
        _ <- TestClock.adjust(5.seconds)
        _ <- producerFiber.interrupt
        topic <- Consumer.subscribeAnd(Subscription.topics("items"))
          .plainStream(Serde.string, Serde.string)
          .take(5)
          .runCollect
          .map(chunk => chunk.map(x => (x.key, x.value)).toSeq)
      } yield assert(topic)(equalTo(expectedMessages))
    }.provideCustomLayer(testConsumerProducerLayer) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
