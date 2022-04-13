package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{Data, ViewsCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.StreamProcessing.{lastFiveMinuteStoreName, lastMinuteStoreName, lastPastStoreName}

import java.time.Instant
import scala.jdk.CollectionConverters._


object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("movies" / Segment) { id: String =>
        get {
          val kvStore1Minute: ReadOnlyWindowStore[String, Long] = streams.store(lastMinuteStoreName,QueryableStoreTypes.windowStore[String,Long]())
          val to = Instant.now()
          val from_one = to.minusSeconds(60)

          val kvStore5Minute: ReadOnlyWindowStore[String, Long] = streams.store(lastFiveMinuteStoreName,QueryableStoreTypes.windowStore[String,Long]())
          val from_five = to.minusSeconds(300)

          val kvStoreAll: ReadOnlyWindowStore[String, Long] = streams.store(lastPastStoreName,QueryableStoreTypes.windowStore[String,Long]())

          complete(
            val past: List[Data] = kvStore1Minute.fetchAll(from_one, to).asScala.toList.map(k=>Data(k.value.))
          )
          }
        }
      },
      path("latency" / "beginning") {
        get {
          complete(
            List(0)
          )
        }
      }
    )
  }
}