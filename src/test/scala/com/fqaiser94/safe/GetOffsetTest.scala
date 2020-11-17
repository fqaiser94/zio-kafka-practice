package com.fqaiser94.safe

import com.fqaiser94.safe.Utils._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestConsole, TestEnvironment}
import zio.{Chunk, ZIO}

object GetOffsetTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("Should print out messages from items topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2")))

        brokerList <- ZIO.access[Kafka](_.get.bootstrapServers.mkString(","))
        offsetMap = Utils.getOffset(brokerList, "items")
      } yield assert(offsetMap)(equalTo(Map(new TopicPartition("items", 0) -> 2L)))
    } @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
      .provideSomeLayer(Kafka.test ++ Blocking.live ++ Clock.live ++ TestConsole.debug)

}
