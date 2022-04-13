package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreForMovies(
                              sum: Float,
                              count: Float,
                              meanScore: Float
                            ) {
  def increment(latency: Float) = this.copy(sum = this.sum + latency, count = this.count + 1)

  def computeMeanMovies = this.copy(
    meanScore = this.sum / this.count
  )
}

object MeanScoreForMovies {
  implicit val format: OFormat[MeanScoreForMovies] = Json.format[MeanScoreForMovies]

  def empty: MeanScoreForMovies = MeanScoreForMovies(0, 0, 0)
}



