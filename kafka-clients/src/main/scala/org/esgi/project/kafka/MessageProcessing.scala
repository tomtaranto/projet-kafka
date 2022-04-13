package org.esgi.project.kafka

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.esgi.project.kafka.models.ConnectionEvent

import java.time.{Duration, OffsetDateTime}
import java.util
import java.util.UUID
import java.util.concurrent.{ScheduledFuture, TimeUnit}

object MessageProcessing extends PlayJsonSupport with SimpleSchedulingLoops with KafkaConfig {
  // TODO: fill your first name & last name
  val yourFirstName: String = "TARANTO"
  val yourLastName: String = "Tom"

  val applicationName = s"simple-app-$yourFirstName-$yourLastName-2"
  val topicName: String = "connection-events"

  def run(): ScheduledFuture[_] = {
//    producerScheduler.schedule(producerLoop, 1, TimeUnit.SECONDS)
    consumerScheduler.schedule(consumerLoop, 1, TimeUnit.SECONDS)

  }

  // TODO: implement message production
  def producerLoop = {
    // Instantiating the producer
    // toSerializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    println("before producer")
    val producer = new KafkaProducer[String, ConnectionEvent](buildProducerProperties, toSerializer[String], toSerializer[ConnectionEvent])
    //    var id = 1;
    println("after producer")
    // TODO: use this loop to produce messages
    while (!producerScheduler.isShutdown) {
      //      println(s"loop : $id")
      // TODO: prepare a ProducerRecord with a String as key composed of firstName and lastName
      // TODO: as well as a message which is a ConnectionEvent
      val key: String = s"$yourFirstName-$yourLastName"
      val record: ConnectionEvent = new ConnectionEvent(UUID.randomUUID().toString,
        "Tom",
        "Taranto",
        OffsetDateTime.now())
//      id+=1;

      // TODO: send the record to Kafka

      producer.send(new ProducerRecord[String, ConnectionEvent](topicName, key, record))

      // slow down the loop to not monopolize your CPU
      Thread.sleep(1000)
    }

    producer.close()
  }

  // Message consumption
  def consumerLoop = {
    // Instantiating the consumer
    // toDeserializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    val consumer = new KafkaConsumer[String, ConnectionEvent](buildConsumerProperties, toDeserializer[String], toDeserializer[ConnectionEvent])
    // TODO: subscribe to the topic to receive the messages - topicName contains the name of the topic.
    consumer.subscribe(util.Arrays.asList(topicName))

    // Consuming messages on our topic
    while (!consumerScheduler.isShutdown) {
      // TODO: fetch messages from the kafka cluster
      val records: ConsumerRecords[String, ConnectionEvent] = consumer.poll(Duration.ofMillis(100))
      consumer.seekToBeginning(consumer.assignment())
      // TODO: print the received messages
      println(records)
      records.forEach(record => {if (record.key() == s"$yourFirstName-$yourLastName") {println(s"${record.value().firstName.toString} ${record.value().lastName} ${record.value().timestamp.toString}")}})

      consumer.commitAsync()
    }

    consumer.close()
  }
}
