package com.fqaiser94.safe

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.TestAspect.timeout
import zio.test.TestAspectAtLeastR
import zio.test.environment.Live
import zio.{Chunk, Ref, ZIO}

import scala.jdk.CollectionConverters.{iterableAsScalaIterableConverter, mapAsScalaMapConverter, seqAsJavaListConverter}

object Utils {

  val kafkaTestTimeout: TestAspectAtLeastR[Live] = timeout(60.seconds)

  /**
   * Returns numMessages from the beginning of the given topic.
   * Note that this will block until numMessages exist in topic.
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

  /**
   * Returns a map of Partition -> LatestOffset for a given topic.
   */
  val getLatestOffsets: String => ZIO[Kafka, Throwable, Map[Int, Long]] = (topic: String) => for {
    bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
    config = {
      val p = new Properties
      p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
      p.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "getLatestOffset")
      p
    }
    consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)
    partitionInfos = consumer.listTopics.asScala.filter { case (k, _) => k == topic }.values.flatMap(_.asScala).toSeq
    topicPartitions = partitionInfos.sortBy(_.partition).flatMap {
      case p if p.leader == null => None
      case p => Some(new TopicPartition(p.topic, p.partition))
    }
    res = consumer.endOffsets(topicPartitions.asJava).asScala.map {
      case (topicPartition, endOffset) => (topicPartition.partition(), endOffset.asInstanceOf[Long])
    }.toMap
  } yield res

  /**
   * Returns all messages from the beginning of the given topic.
   */
  val consumeAllMessagesFromKafka: String => ZIO[Kafka, Throwable, Chunk[(String, String)]] =
    (topic: String) => for {
      bootstrapServers <- ZIO.access[Kafka](_.get.bootstrapServers)
      settings = ConsumerSettings(bootstrapServers)
        .withGroupId("test-consumer")
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      consumedOffsets <- Ref.make(Map.empty[Int, Long])
      latestOffsets <- getLatestOffsets(topic)
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
   * Produces given messages to kafka.
   * Note that this will blocks until broker acknowledgement.
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
