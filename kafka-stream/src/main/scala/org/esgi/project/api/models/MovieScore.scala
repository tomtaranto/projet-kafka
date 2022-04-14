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
