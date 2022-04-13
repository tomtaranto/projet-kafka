package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreForMovies(
                              sum: Long,
                              count: Long,
                              meanScore: Long
                            ) {
  def increment(latency: Long) = this.copy(sum = this.sum + latency, count = this.count + 1)

  def computeMeanLatency = this.copy(
    meanScore = this.sum / this.count
  )
}

object MeanScoreForMovies {
  implicit val format: OFormat[MeanScoreForMovies] = Json.format[MeanScoreForMovies]

  def empty: MeanScoreForMovies = MeanScoreForMovies(0, 0, 0)
}



