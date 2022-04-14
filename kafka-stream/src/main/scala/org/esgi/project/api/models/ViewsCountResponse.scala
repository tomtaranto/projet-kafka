package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewsCountResponse(
                             _id: Option[Int],
                             title: Option[String],
                             view_count: Option[Float],
                             stats: Stats,
                     )

object ViewsCountResponse {
  implicit val format: OFormat[ViewsCountResponse] = Json.format[ViewsCountResponse]
}


