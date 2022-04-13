package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.common.serialization.Serdes.String
import org.apache.kafka.streams.kstream.{KTable, Windowed}
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.state.QueryableStoreTypes.windowStore
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.MeanLatencyForURL

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * -------------------
 * Part.3 of exercise: Interactive Queries
 * -------------------
 */
object WebServer extends PlayJsonSupport {

  val thirtySecondsStoreName: String = "VisitsOfLast30Seconds"
  val lastMinuteStoreName = "VisitsOfLastMinute"
  val lastFiveMinutesStoreName = "VisitsOfLast5Minutes"

  val thirtySecondsByCategoryStoreName: String = "VisitsOfLast30SecondsByCategory"
  val lastMinuteByCategoryStoreName = "VisitsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "VisitsOfLast5MinutesByCategory"
  val meanLatencyForURLStoreName = "MeanLatencyForURL"
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          period match {
            case "30s" =>
              // TODO: load the store containing the visits count of the last 30 seconds and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore30Seconds: ReadOnlyWindowStore[String, Long] = streams.store(thirtySecondsStoreName,QueryableStoreTypes.windowStore[String,Long]())
//              val keys = kvStore30Seconds.all().asScala.map(k=>VisitCountResponse(k.key.key(),k.value)).toList.distinct
              val to = Instant.now()
              val from = to.minusSeconds(30)
              complete(
                // TODO: output a list of VisitCountResponse objects
                 kvStore30Seconds.fetchAll(from,to).asScala.toList.map(k=>VisitCountResponse(k.key.key(),k.value))
              )
            case "1m" =>
              // TODO: load the store containing the visits count of the last minute and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore1Minute: ReadOnlyWindowStore[String, Long] = streams.store(lastMinuteStoreName,QueryableStoreTypes.windowStore[String,Long]())
              val to = Instant.now()
              val from = to.minusSeconds(60)
              complete(
                // TODO: output a list of VisitCountResponse objects

                kvStore1Minute.fetchAll(from,to).asScala.toList.map(k=>VisitCountResponse(k.key.key(),k.value))
              )
            case "5m" =>
              // TODO: load the store containing the visits count of the last five minutes and query it to
              // TODO: fetch the keys of the last window and its info
              val kvStore5Minute: ReadOnlyWindowStore[String, Long] = streams.store(lastFiveMinutesStoreName,QueryableStoreTypes.windowStore[String,Long]())
              val to = Instant.now()
              val from = to.minusSeconds(300)
              complete(
                // TODO: output a list of VisitCountResponse objects
                kvStore5Minute.fetchAll(from,to).asScala.toList.map(k=>VisitCountResponse(k.key.key(),k.value))
              )
            case _ =>
              // unhandled period asked
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },
      path("latency" / "beginning") {
        get {
          // TODO: output the mean latency per URL since the start of the app
          val kvStoreMeanLatencyPerURL: ReadOnlyKeyValueStore[String, MeanLatencyForURL] = streams.store(meanLatencyForURLStoreName, QueryableStoreTypes.keyValueStore[String, MeanLatencyForURL]())

          complete(
            // TODO: output a list of MeanLatencyForURLResponse objects
            kvStoreMeanLatencyPerURL.all().asScala.toList.map(k=>MeanLatencyForURLResponse(k.key, k.value.meanLatency))
          )
        }
      }
    )
  }
}
