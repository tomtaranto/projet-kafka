package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewsCountResponse(
                             _id: Int,
                             title: String,
                             view_count: Float,
                             stats: Stats,
                     )

object ViewsCountResponse {
  implicit val format: OFormat[ViewsCountResponse] = Json.format[ViewsCountResponse]
}


