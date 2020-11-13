package com.fqaiser94

import com.dimafeng.testcontainers.KafkaContainer
import zio._
import zio.blocking.{Blocking, effectBlocking}


package object safe {

  type Kafka = Has[Kafka.Service]

  object Kafka {

    trait Service {
      def bootstrapServers: List[String]

      def stop(): UIO[Unit]
    }

    object Service {
      val live: Service = new Service {
        def bootstrapServers: List[String] = List(s"localhost:9092")

        def stop(): UIO[Unit] = UIO.unit
      }
    }

    val any: ZLayer[Kafka, Nothing, Kafka] = ZLayer.requires[Kafka]

    val live: Layer[Nothing, Kafka] = ZLayer.succeed(Service.live)

    val test: Layer[Nothing, Kafka] = Blocking.live >>> ZManaged.make {
      effectBlocking {
        val container = new KafkaContainer()
        container.start()
        container
      }.orDie.map(container => new Service {
        def bootstrapServers: List[String] = container.bootstrapServers :: Nil

        def stop(): UIO[Unit] = ZIO.effectTotal(container.stop())
      })
    }(release = _.stop()).toLayer
  }

}
