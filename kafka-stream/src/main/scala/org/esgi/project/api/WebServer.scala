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
          println(s"request for id : $id")
          val kvStore: ReadOnlyWindowStore[Int, ViewAggregate] = streams.store(storeMovieID, QueryableStoreTypes.windowStore[Int, ViewAggregate]())
          val to: Instant = Instant.now()
          val from_one: Instant = to.minusSeconds(60)
          val from_five: Instant = to.minusSeconds(5*60)

          val keys = kvStore.all().asScala.map(_.key.key()).toList.distinct

          complete(
            if (keys.filter(x=>x==id.toInt).toList.isEmpty)
              List("Movie not found");
            else
              keys
                .filter(x=> x ==id.toInt)
                .map(x =>{
                  val row1min: Iterator[Data] = kvStore.fetch(x,from_one, to).asScala.map((v) =>{
                    Data(Some(v.value.categorie1_count),Some(v.value.categorie2_count),Some(v.value.categorie3_count))
                  })
                  println(row1min.take(1).toList.headOption)
                  val row5min: Iterator[Data] = kvStore.fetch(x,from_five, to).asScala.map((v) =>{
                    Data(Some(v.value.categorie1_count),Some(v.value.categorie2_count),Some(v.value.categorie3_count))
                  })
                  val rowAll: Iterator[Data] = kvStore.fetch(x,to.minusSeconds(100000000),to).asScala.map((v) =>{
                    Data(Some(v.value.categorie1_count),Some(v.value.categorie2_count),Some(v.value.categorie3_count))
                  })

                  ViewsCountResponse(Some(x),
                    kvStore.fetch(x,to.minusSeconds(100000000),to).asScala.map(v=>v.value.title).take(1).toList.headOption,
                    Some(0),
                      Stats( rowAll.toList.headOption.foldLeft(Data.empty:Data)((a,b)=>a.addData(b.start_only.getOrElse(0f),b.half.getOrElse(0f), b.full.getOrElse(0f))),
                        row1min.take(1).toList.headOption.getOrElse(Data.empty),
                        row5min.take(1).toList.headOption.foldLeft(Data.empty:Data)((a,b)=>a.addData(b.start_only.getOrElse(0f),b.half.getOrElse(0f), b.full.getOrElse(0f)))
                       )
                    )
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

