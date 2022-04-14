package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewsCountResponse(
                             _id: Option[Int],
                             title: Option[String],
                             view_count: Option[Float],
                             stats: Option[Stats],
                     ){
  def computeTotal = this.copy(this._id,
    this.title,
    Some(this.stats.get.past.get.full+this.stats.get.past.get.half +this.stats.get.past.get.start_only),
    this.stats
  )
}

object ViewsCountResponse {
  implicit val format: OFormat[ViewsCountResponse] = Json.format[ViewsCountResponse]
}


