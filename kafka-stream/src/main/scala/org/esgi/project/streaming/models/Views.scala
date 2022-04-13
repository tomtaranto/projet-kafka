package org.esgi.project.streaming.models
import play.api.libs.json.{Json, OFormat}

case class Views(
                  _id: Int,
                  title: String,
                  view_category: String
                )

object Views{
  implicit val format: OFormat[Views] = Json.format[Views]
}