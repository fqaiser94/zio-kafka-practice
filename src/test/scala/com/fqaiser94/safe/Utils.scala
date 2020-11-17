package com.fqaiser94.safe

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.{Chunk, Ref, ZIO}

import scala.jdk.CollectionConverters.{iterableAsScalaIterableConverter, mapAsScalaMapConverter, seqAsJavaListConverter}

object Utils {

  /**
   * Blocks until numMessages have been consumed
   */
  val consumeMessagesFromKafka: (Int, String) => ZIO[Kafka, Throwable, Chunk[(String, String)]] =
    (numMessages: Int, topic: String) => for {
      bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
      settings = ConsumerSettings(bootstrapServers)
        .withGroupId("test-consumer")
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      messages <- Consumer.make(settings).use(_
        .subscribeAnd(Subscription.topics(topic))
        .plainStream(Serde.string, Serde.string)
        .take(numMessages)
        .map(x => (x.key, x.value))
        .runCollect).provideSomeLayer(Clock.live ++ Blocking.live)
    } yield messages

  def latestOffset(brokerList: String, topic: String): Map[Int, Long] = {
    val clientId = "GetOffsetShell"
    val config = new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    val consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)
    val partitionInfos = consumer.listTopics.asScala.filter { case (k, _) => k == topic }.values.flatMap(_.asScala).toSeq
    val topicPartitions = partitionInfos.sortBy(_.partition).flatMap {
      case p if p.leader == null => None
      case p => Some(new TopicPartition(p.topic, p.partition))
    }

    consumer
      .endOffsets(topicPartitions.asJava)
      .asScala
      .map { case (topicPartition, endOffset) => (topicPartition.partition(), endOffset.asInstanceOf[Long]) }
      .toMap
  }

  /**
   * Blocks until numMessages have been consumed
   */
  val consumeAllMessagesFromKafka: String => ZIO[Kafka, Throwable, Chunk[(String, String)]] =
    (topic: String) => for {
      bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
      settings = ConsumerSettings(bootstrapServers)
        .withGroupId("test-consumer")
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      consumedOffsets <- Ref.make(Map.empty[Int, Long])
      latestOffsets = latestOffset(bootstrapServers.mkString(","), topic)
      messages <- Consumer.make(settings).use(_
        .subscribeAnd(Subscription.topics(topic))
        .plainStream(Serde.string, Serde.string)
        .takeUntilM(cr => for {
          // don't understand why the last message has an offset that is 1 less than the latest offset
          offsets <- consumedOffsets.updateAndGet(map => map ++ Map(cr.partition -> (cr.offset.offset + 1)))
        } yield offsets == latestOffsets)
        .map(x => (x.key, x.value))
        .runCollect).provideSomeLayer[Kafka](Clock.live ++ Blocking.live)
    } yield messages

  /**
   * Blocks until messages have been produced to Kafka
   */
  val produceMessagesToKafka: Chunk[ProducerRecord[String, String]] => ZIO[Kafka, Throwable, Unit] =
    (messages: Chunk[ProducerRecord[String, String]]) => for {
      bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
      producerSettings = ProducerSettings(bootstrapServers)
      producerManaged = Producer.make[Any, String, String](producerSettings, Serde.string, Serde.string)
      _ <- producerManaged.use(_.produceChunk(messages))
        .provideSomeLayer(Clock.live ++ Blocking.live)
    } yield ()

}
