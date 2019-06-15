package com.kafka

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


object ProduceConsumeApp extends App {

  implicit val system: ActorSystem = ActorSystem("ProduceConsumeApp")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")


  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("my-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())


  val produce = Source(1 to 10)
    .map(_.toString)
    .map { elem =>
      println(s"PlainSinkProducer produce: $elem")
      new ProducerRecord[String, String]("topic1", elem)
    }
    .runWith(Producer.plainSink(producerSettings))


  val consume = Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
    .runForeach { message =>
      val record = message.record
      println(s"Record topic: ${record.topic()}, partition: ${record.partition()}, offset: ${record.offset()}")
      println(s"Record key: ${record.key()}")
      println(s"Record value: ${record.value()}")
      CommittableOffsetBatch(message.committableOffset).commitScaladsl().foreach(co => println(s"record committed"))

    }
}