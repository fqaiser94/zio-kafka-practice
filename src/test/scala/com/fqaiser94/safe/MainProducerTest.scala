package com.fqaiser94.safe

import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.{Duration, durationInt}
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{Live, TestClock, TestEnvironment}
import zio.{Chunk, Fiber, Schedule, URIO, ZIO}

object MainProducerTest extends DefaultRunnableSpec {

  /**
   * Continuously advances TestClock time by testInterval every liveInterval
   */
  def speedUpTime(testIntervals: Duration, liveIntervals: Duration): URIO[TestClock with Live, Fiber.Runtime[Nothing, Long]] = {
    val adjustTestClock = TestClock.adjust(Duration.fromJava(testIntervals))
    val liveRepeatSchedule = Schedule.spaced(Duration.fromJava(liveIntervals))
    Live.withLive(adjustTestClock)(_.repeat(liveRepeatSchedule)).fork
  }

  private val tests = Seq(
    testM("writes a message to kafka") {
      for {
        _ <- TestClock.setTime(0.second)
        _ <- MainProducer.program

        bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
        settings = ConsumerSettings(bootstrapServers)
          .withGroupId("test-consumer")
          .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        msg1 <- Consumer.make(settings).use(_
          .subscribeAnd(Subscription.topics("items"))
          .plainStream(Serde.string, Serde.string)
          .take(1)
          .map(x => (x.key, x.value))
          .runCollect).provideSomeLayer(Clock.live ++ Blocking.live)

      } yield assert(msg1)(equalTo(Chunk((null, "0"))))
    }.provideSomeLayer(Kafka.test ++ Blocking.live ++ TestClock.default) @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)

}
