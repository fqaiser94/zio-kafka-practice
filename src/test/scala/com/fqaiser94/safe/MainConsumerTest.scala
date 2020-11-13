//package com.fqaiser94.safe
//
//import com.fqaiser94.safe.MainConsumer
//import com.fqaiser94.utils.speedUpTime
//import org.apache.kafka.clients.producer.ProducerRecord
//import zio.Chunk
//import zio.kafka.producer.Producer
//import zio.test.Assertion.equalTo
//import zio.test.TestAspect.timeout
//import zio.test.environment.TestConsole
//import zio.test._
//
//object MainConsumerTest extends DefaultRunnableSpec {
//
//  private val tests = Seq(
//    testM("Consumes all messages from kafka topic and prints them to console") {
//      for {
//        _ <- Producer.produceChunk[Any, String, String](Chunk(
//          new ProducerRecord[String, String]("items", "key1", "value1"),
//          new ProducerRecord[String, String]("items", "key2", "value2")
//        ))
//        timeFiber <- speedUpTime(testIntervals = 60.seconds, liveIntervals = 500.millis).fork
//        programFiber <- MainConsumer.program.fork
//        output <- TestConsole.output
//        _ <- timeFiber.interrupt
//        _ <- programFiber.interrupt
//      } yield assert(output.head)(equalTo(
//        """key1:value1
//          |key2:value2
//          |""".stripMargin))
//    }.provideCustomLayer(TestConsole.silent ++ testConsumerProducerLayer) @@ timeout(60.seconds)
//  )
//
//  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
//    suite(super.getClass.getSimpleName.dropRight(1))(tests: _*)
//
//}
