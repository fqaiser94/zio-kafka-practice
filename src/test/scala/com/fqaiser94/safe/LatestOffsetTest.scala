package com.fqaiser94.safe

import com.fqaiser94.safe.Utils._
import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.admin.AdminClient.NewTopic
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.{TestConsole, TestEnvironment}
import zio.{Chunk, ZIO}

object LatestOffsetTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("latestOffset should return map of offsets from topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2")))

        brokerList <- ZIO.access[Kafka](_.get.bootstrapServers.mkString(","))
        offsetMap = Utils.latestOffset(brokerList, "items")
      } yield assert(offsetMap)(equalTo(Map(0 -> 2L)))
    } @@ timeout(60.seconds),
    testM("consumeAllMessagesFromKafka should consume all message from kafka") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2"),
          new ProducerRecord[String, String]("items", "key3", "value3")))

        messages <- consumeAllMessagesFromKafka("items")
      } yield assert(messages)(equalTo(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3"))))
    } @@ timeout(60.seconds),
    testM("consumeAllMessagesFromKafka should consume all message from kafka across multiple partitions") {
      for {
        brokerList <- ZIO.access[Kafka](_.get.bootstrapServers)
        _ <- AdminClient.make(AdminClientSettings(brokerList)).use(client => client.createTopic(NewTopic("items", 2, 1)))
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", 0, "key1", "value1"),
          new ProducerRecord[String, String]("items", 1, "key2", "value2"),
          new ProducerRecord[String, String]("items", 1, "key3", "value3")))

        messages <- consumeAllMessagesFromKafka("items")
      } yield assert(messages)(hasSameElements(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3"))))
    } @@ timeout(60.seconds)
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
      .provideSomeLayer(Kafka.test ++ Blocking.live ++ Clock.live ++ TestConsole.debug)

}
