package com.fqaiser94.safe

import com.fqaiser94.safe.Utils._
import org.apache.kafka.clients.producer.ProducerRecord
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.admin.AdminClient.NewTopic
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.test.Assertion.{equalTo, hasSameElements}
import zio.test._
import zio.test.environment.{TestConsole, TestEnvironment}
import zio.{Chunk, ZIO}

object UtilsTest extends DefaultRunnableSpec {

  private val tests = Seq(
    testM("getLatestOffsets should return map of offsets for single-partition topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2")))

        offsetMap <- getLatestOffsets("items")
      } yield assert(offsetMap)(equalTo(Map(0 -> 2L)))
    } @@ kafkaTestTimeout,
    testM("getLatestOffsets should return map of offsets for multi-partition topic") {
      for {
        brokerList <- ZIO.access[Kafka](_.get.bootstrapServers)
        _ <- AdminClient.make(AdminClientSettings(brokerList)).use(client => client.createTopic(NewTopic("items", 2, 1)))
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", 0, "key1", "value1"),
          new ProducerRecord[String, String]("items", 1, "key2", "value2"),
          new ProducerRecord[String, String]("items", 1, "key3", "value3")))

        offsetMap <- getLatestOffsets("items")
      } yield assert(offsetMap)(equalTo(Map(0 -> 1L, 1 -> 2L)))
    } @@ kafkaTestTimeout,
    testM("consumeAllMessagesFromKafka should consume all messages from singe-partitioned topic") {
      for {
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", "key1", "value1"),
          new ProducerRecord[String, String]("items", "key2", "value2"),
          new ProducerRecord[String, String]("items", "key3", "value3")))

        messages <- consumeAllMessagesFromKafka("items")
      } yield assert(messages)(equalTo(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3"))))
    } @@ kafkaTestTimeout,
    testM("consumeAllMessagesFromKafka should consume all message from multi-partitioned topic") {
      for {
        brokerList <- ZIO.access[Kafka](_.get.bootstrapServers)
        _ <- AdminClient.make(AdminClientSettings(brokerList)).use(client => client.createTopic(NewTopic("items", 2, 1)))
        _ <- produceMessagesToKafka(Chunk(
          new ProducerRecord[String, String]("items", 0, "key1", "value1"),
          new ProducerRecord[String, String]("items", 1, "key2", "value2"),
          new ProducerRecord[String, String]("items", 1, "key3", "value3")))

        messages <- consumeAllMessagesFromKafka("items")
      } yield assert(messages)(hasSameElements(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3"))))
    } @@ kafkaTestTimeout
  )

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
      .provideSomeLayer(Kafka.test ++ Blocking.live ++ Clock.live ++ TestConsole.debug)

}
