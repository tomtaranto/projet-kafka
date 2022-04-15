package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewsCountResponse(
                               _id: Option[Int],
                               title: Option[String],
                               view_count: Option[Float],
                               stats: Stats,
                             ) {
  def computeTotal() = {
    this.copy(view_count = Some(this.stats.past.full.getOrElse(0f) + this.stats.past.half.getOrElse(0f) + this.stats.past.start_only.getOrElse(0f)))
  }
}

object ViewsCountResponse {
  implicit val format: OFormat[ViewsCountResponse] = Json.format[ViewsCountResponse]
}


