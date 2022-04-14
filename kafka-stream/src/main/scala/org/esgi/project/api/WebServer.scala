package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{Data, Stats, ViewsCountResponse}
import org.esgi.project.streaming.StreamProcessing.storeMovieID
import org.esgi.project.streaming.models.ViewAggregate

import java.time.Instant
import scala.jdk.CollectionConverters.asScalaIteratorConverter


object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("movies" / Segment) { id: String =>
        get {
          val kvStore: ReadOnlyWindowStore[Int, ViewAggregate] = streams.store(storeMovieID, QueryableStoreTypes.windowStore[Int, ViewAggregate]())
          val to = Instant.now()
          val from_one = to.minusSeconds(60)
          val from_five = to.minusSeconds(5*60)

          val keys = kvStore.all().asScala.map(_.key.key()).toList.distinct

          complete(

            keys
              .filter(x=> x ==id.toInt)
              .map(x =>{
                val row1min = kvStore.fetch(x,from_one, to).asScala.map((v) =>{
                  Data(Some(v.value.categorie1_count).getOrElse(0),Some(v.value.categorie2_count).getOrElse(0),Some(v.value.categorie3_count).getOrElse(0))
                })
//                println(row1min.take(1).toList.headOption)
                val row5min = kvStore.fetch(x,from_five, to).asScala.map((v) =>{
                  Data(Some(v.value.categorie1_count).getOrElse(0),Some(v.value.categorie2_count).getOrElse(0),Some(v.value.categorie3_count).getOrElse(0))
                })
                val rowAll = kvStore.fetch(x,0,to.toEpochMilli).asScala.map((v) =>{
                  Data(Some(v.value.categorie1_count).getOrElse(0),Some(v.value.categorie2_count).getOrElse(0),Some(v.value.categorie3_count).getOrElse(0))
                })
                println(rowAll.take(1).toList.headOption)

                ViewsCountResponse(Some(x),
                  kvStore.fetch(x,to.minusSeconds(100000000),to).asScala.map(v=>v.value.title).take(1).toList.headOption,
                  Some(0),
                  Some(
                    Stats(row1min.take(1).toList.headOption,
                    row5min.take(1).toList.headOption,
                    rowAll.take(1).toList.headOption)
                  )).computeTotal
              })

          )
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
