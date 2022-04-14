package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}


case class Stats(
                past: Option[Data],
                last_minute: Option[Data],
                last_five_minutes: Option[Data]
                )

case class Data(
               start_only: Float,
               half: Float,
               full: Float
                )

object Stats {
  implicit val format: OFormat[Stats] = Json.format[Stats]


}

object Data {
  implicit val format: OFormat[Data] = Json.format[Data]
}