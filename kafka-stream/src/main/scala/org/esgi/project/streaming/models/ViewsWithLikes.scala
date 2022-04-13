package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewsWithLikes(
                             _id: Int,
                             title: String,
                             view_category: String,
                             score: Long
                           )

object ViewsWithLikes {
  implicit val format: OFormat[ViewsWithLikes] = Json.format[ViewsWithLikes]

}
