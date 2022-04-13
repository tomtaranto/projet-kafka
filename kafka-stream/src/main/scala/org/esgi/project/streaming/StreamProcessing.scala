package org.esgi.project.streaming
import org.apache.kafka.streams.kstream.TimeWindows

import java.time.Duration
import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, Printed, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{Likes, MeanScoreForMovies, Views, ViewsWithLikes}

import java.io.InputStream
import java.time.Duration
import java.util.Properties
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._

object StreamProcessing extends PlayJsonSupport {
  val applicationName = "Foresight"

  val likesTopicName: String = "likes"
  val viewsTopicName: String = "views"

  val lastMinuteStoreName: String = "viewsLastMinuteCategories"
  val lastFiveMinuteStoreName: String = "viewsLastFiveMinuteCategories"
  val lastPastStoreName: String = "viewsLastPastategories"

  val meanScoreStoreName: String = "meanScoreStoreName"

  val props = buildProperties
  val builder: StreamsBuilder = new StreamsBuilder

  val likes: KStream[String, Likes] = builder.stream[String, Likes](likesTopicName)
  val views: KStream[String, Views] = builder.stream[String, Views](viewsTopicName)

  // Nombre de vue par film
  val viewsGroupedByTitle: KGroupedStream[String, Views] = views.groupBy((_, value) => value.title)
  val viewsGroupedByTitleCategorie: KGroupedStream[String, Views] = views.selectKey((k, v) => v.title+v.view_category)
    .groupByKey

  val windows1: TimeWindows = TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(4))
  val viewsOfLast1Minute: KTable[Windowed[String], Long] = viewsGroupedByTitleCategorie.windowedBy(windows1).count()(Materialized.as(lastMinuteStoreName))

  val windows5: TimeWindows = TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(4))
  val viewsOfLast5Minute: KTable[Windowed[String], Long] = viewsGroupedByTitleCategorie.windowedBy(windows5).count()(Materialized.as(lastFiveMinuteStoreName))

  val windowsPast: TimeWindows = TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(4))
  val viewsOfLastPast: KTable[Windowed[String], Long] = viewsGroupedByTitleCategorie.windowedBy(windowsPast).count()(Materialized.as(lastPastStoreName))

  val viewsWithLikes: KStream[String, ViewsWithLikes] = views.join(likes)((v:Views, l:Likes) => ViewsWithLikes(v._id, v.title, v.view_category, l.score), JoinWindows.of(Duration.ofMinutes(2)))

  val meanScoreMovie: KTable[String, MeanScoreForMovies] = viewsWithLikes.groupBy(((k,v)=> v.title)).aggregate(MeanScoreForMovies.empty)((_,v,agg) => {agg.increment(v.score)}.computeMeanMovies)(Materialized.as(meanScoreStoreName))

  meanScoreMovie.inner.toStream().print(Printed.toSysOut())



  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run {
        streams.close
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}
