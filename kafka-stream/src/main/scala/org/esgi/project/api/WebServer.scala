package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{Data, MovieScore, MovieView, Stats, ViewsCountResponse}
import org.esgi.project.streaming.StreamProcessing.{meanScoreStoreName, storeCountViews, storeMovieID}
import org.esgi.project.streaming.models.{MeanScoreForMovies, ViewAggregate}

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
          val from_five: Instant = to.minusSeconds(5 * 60)

          val keys = kvStore.all().asScala.map(_.key.key()).toList.distinct

          complete(
            if (keys.filter(x => x == id.toInt).toList.isEmpty)
              List("Movie not found");
            else
              keys
                .filter(x => x == id.toInt)
                .map(x => {
                  val row1min: Iterator[Data] = kvStore.fetch(x, from_one, to).asScala.map((v) => {
                    Data(Some(v.value.categorie1_count), Some(v.value.categorie2_count), Some(v.value.categorie3_count))
                  })
//                  println(row1min.take(1).toList.headOption)
                  val row5min: Iterator[Data] = kvStore.fetch(x, from_five, to).asScala.map((v) => {
                    Data(Some(v.value.categorie1_count), Some(v.value.categorie2_count), Some(v.value.categorie3_count))
                  })
                  val rowAll: Iterator[Data] = kvStore.fetch(x, to.minusSeconds(100000000), to).asScala.map((v) => {
                    Data(Some(v.value.categorie1_count), Some(v.value.categorie2_count), Some(v.value.categorie3_count))
                  })
//                  val malist = row5min.toList
//                  println("list : ",malist)
//
//                  println("res : ",malist.foldLeft(Data.empty: Data){(acc, data) =>  acc.addData(data.start_only.getOrElse(0f), data.half.getOrElse(0f), data.full.getOrElse(0f))})
//
//                  println(malist.foldLeft(Data.empty: Data){(acc, data) =>
//                    Data(Some(acc.start_only.getOrElse(0f) + data.start_only.getOrElse(0f)),
//                      Some(acc.half.getOrElse(0f) + data.half.getOrElse(0f)),
//                      Some(acc.full.getOrElse(0f)+data.full.getOrElse(0f)))})
//                  println(kvStore.all().asScala.filter(v => v.value._id.toString == id).foldLeft(Data.empty: Data)((a, b) => a.addData(b.value.categorie1_count, b.value.categorie2_count, b.value.categorie3_count)))

                  ViewsCountResponse(Some(x),
                    kvStore.fetch(x, 0, Instant.now().toEpochMilli).asScala.map(v => v.value.title).take(1).toList.headOption,
                    Some(0),
                    Stats(kvStore.all().asScala.filter(v => v.value._id.toString == id).foldLeft(Data.empty: Data)((a, b) => a.addData(b.value.categorie1_count, b.value.categorie2_count, b.value.categorie3_count)),
                      row1min.toList.headOption.getOrElse(Data.empty),
                      row5min.toList.foldLeft(Data.empty: Data)((a, b) => a.addData(b.start_only.getOrElse(0f), b.half.getOrElse(0f), b.full.getOrElse(0f)))
                    )
                  ).computeTotal()
                })

          )
        }
      },
      path("stats" / "ten" / "best" / "score") {
        println("Requete ")
        val kvStore: ReadOnlyKeyValueStore[String, MeanScoreForMovies] = streams.store(meanScoreStoreName, QueryableStoreTypes.keyValueStore[String, MeanScoreForMovies]())

        get {
          complete(

            kvStore.all().asScala.toList
              .map((k) => MovieScore(k.value.title, k.value.meanScore))
              .toList.distinct.sortBy(_.score)(implicitly[Ordering[Float]]).reverse.take(10)
          )
        }
      },
      path("stats" / "ten" / "worst" / "score") {
        val kvStore: ReadOnlyKeyValueStore[String, MeanScoreForMovies] = streams.store(meanScoreStoreName, QueryableStoreTypes.keyValueStore[String, MeanScoreForMovies]())

        get {
          complete(

            kvStore.all().asScala.toList
              .map((k) => MovieScore(k.value.title, k.value.meanScore))
              .toList.distinct.sortBy(_.score)(implicitly[Ordering[Float]]).take(10)
          )
        }
      },
      path("stats" / "ten" / "worst" / "views") {
        val kvStore: ReadOnlyKeyValueStore[String, Long] = streams.store(storeCountViews, QueryableStoreTypes.keyValueStore[String, Long]())

        get {
          complete(

            kvStore.all().asScala.toList
              .map((k) => MovieView(k.key, k.value))
              .toList.distinct.sortBy(_.views)(implicitly[Ordering[Long]]).take(10)
          )
        }
      },
      path("stats" / "ten" / "best" / "views") {
        val kvStore: ReadOnlyKeyValueStore[String, Long] = streams.store(storeCountViews, QueryableStoreTypes.keyValueStore[String, Long]())

        get {
          complete(

            kvStore.all().asScala.toList
              .map((k) => MovieView(k.key, k.value))
              .toList.distinct.sortBy(_.views)(implicitly[Ordering[Long]]).reverse.take(10)
          )
        }
      }

    )
  }

}

