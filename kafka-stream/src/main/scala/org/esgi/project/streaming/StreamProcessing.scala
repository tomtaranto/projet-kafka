package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, Printed, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{MeanLatencyForURL, Metric, Visit, VisitWithLatency}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  // TODO: Predeclared store names to be used, fill your first & last name
  val yourFirstName: String = "Tom"
  val yourLastName: String = "TARANTO"

  val applicationName = s"web-events-stream-app-$yourFirstName-$yourLastName"
  val visitsTopicName: String = "visits"
  val metricsTopicName: String = "metrics"

  val thirtySecondsStoreName: String = "VisitsOfLast30Seconds"
  val lastMinuteStoreName = "VisitsOfLastMinute"
  val lastFiveMinutesStoreName = "VisitsOfLast5Minutes"

  val thirtySecondsByCategoryStoreName: String = "VisitsOfLast30SecondsByCategory"
  val lastMinuteByCategoryStoreName = "VisitsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "VisitsOfLast5MinutesByCategory"
  val meanLatencyForURLStoreName = "MeanLatencyForURL"

  val props = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // TODO: declared topic sources to be used
  val visits: KStream[String, Visit] = builder.stream[String, Visit](visitsTopicName)
  val metrics: KStream[String, Metric] = builder.stream[String, Metric](metricsTopicName)

  /**
   * -------------------
   * Part.1 of exercise
   * -------------------
   */
  // TODO: repartition visits per URL
  val visitsGroupedByUrl: KGroupedStream[String, Visit] = visits.groupBy((_, value) => value.url)
  println(visitsGroupedByUrl)
  // TODO: implement a computation of the visits count per URL for the last 30 seconds,
  // TODO: the last minute and the last 5 minutes

  import org.apache.kafka.streams.kstream.TimeWindows
  import java.time.Duration

  val windows30: TimeWindows = TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(4))
  val visitsOfLast30Seconds: KTable[Windowed[String], Long] =visitsGroupedByUrl.windowedBy(windows30).count()(Materialized.as(thirtySecondsStoreName))
//  println(visitsOfLast30Seconds.toStream.print(Printed.toSysOut()))
  val windows1: TimeWindows = TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(4))
  val visitsOfLast1Minute: KTable[Windowed[String], Long] = visitsGroupedByUrl.windowedBy(windows1).count()(Materialized.as(lastMinuteStoreName))
  val windows5: TimeWindows = TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(20))
  val visitsOfLast5Minute: KTable[Windowed[String], Long] = visitsGroupedByUrl.windowedBy(windows5).count()(Materialized.as(lastFiveMinutesStoreName))

  /**
   * -------------------
   * Part.2 of exercise
   * -------------------
   */
  // TODO: repartition visits topic per category instead (based on the 2nd part of the URLs)
//  val visitsGroupedByCategory: KGroupedStream[String, Visit] = visits.filter((key, value) => value.url.startsWith("/store")).groupBy((key, value) => value.url.split('/')(1))
  val visitsGroupedByCategory: KGroupedStream[String, Visit] = visits.filter((k,v) => v.url.startsWith("/store/"))
    .groupBy((k,v) => v.url.split('/')(2))
//  println(visitsGroupedByCategory.inner.count().toStream.print(Printed.toSysOut()))
  // TODO: implement a computation of the visits count per category for the last 30 seconds,
  // TODO: the last minute and the last 5 minutes
  val visitsOfLast30SecondsByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory.windowedBy(windows30).count()(Materialized.as(thirtySecondsByCategoryStoreName))

  val visitsOfLast1MinuteByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory.windowedBy(windows1).count()(Materialized.as(lastMinuteByCategoryStoreName))

  val visitsOfLast5MinuteByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory.windowedBy(windows5).count()(Materialized.as(lastFiveMinutesByCategoryStoreName))

  // TODO: implement a join between the visits topic and the metrics topic,
  // TODO: knowing the key for correlated events is currently the same UUID (and the same id field).
  // TODO: the join should be done knowing the correlated events are emitted within a 5 seconds latency.
  // TODO: the outputted message should be a VisitWithLatency object.
  val visitsWithMetrics: KStream[String, VisitWithLatency] = visits.join(metrics)((visit:Visit, metric:Metric) => VisitWithLatency(visit._id, visit.timestamp, visit.sourceIp, visit.url, metric.latency), JoinWindows.of(Duration.ofMinutes(2)))
//  visitsWithMetrics.foreach((k,v)=>println(v.url,v._id,v.latency,v.sourceIp,v.timestamp))

  // TODO: based on the previous join, compute the mean latency per URL
  val meanLatencyPerUrl: KTable[String, MeanLatencyForURL] = visitsWithMetrics.groupBy(((k,v)=> v.url)).aggregate(MeanLatencyForURL.empty)((_,v,agg) => {agg.increment(v.latency)}.computeMeanLatency)(Materialized.as(meanLatencyForURLStoreName))
//  println(meanLatencyPerUrl.toStream.print(Printed.toSysOut()))
//  // -------------------------------------------------------------
//  // TODO: now that you're here, materialize all of those KTables
//  // TODO: to stores to be able to query them in Webserver.scala
//  // -------------------------------------------------------------



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
