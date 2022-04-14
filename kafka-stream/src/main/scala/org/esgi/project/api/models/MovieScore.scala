package org.esgi.project.api.models

import org.esgi.project.streaming.models.ViewsWithLikes
import play.api.libs.json.{Json, OFormat}

case class MovieScore(
                       title: String,
                       score: Float
                     )

object MovieScore {
  implicit val format: OFormat[MovieScore] = Json.format[MovieScore]

}


case class MovieView(
                       title: String,
                       views: Long
                     )

object MovieView {
  implicit val format: OFormat[MovieView] = Json.format[MovieView]

}
