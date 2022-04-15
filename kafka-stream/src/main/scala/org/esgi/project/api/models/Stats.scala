package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}


case class Stats(
                  past: Data,
                  last_minute: Data,
                  last_five_minutes: Data
                )

case class Data(
                 start_only: Option[Float],
                 half: Option[Float],
                 full: Option[Float]
               ) {

  def addData(y1: Float, y2: Float, y3: Float) = {
    this.copy(start_only = Some(y1 + this.start_only.getOrElse(0f)),
      half = Some(y2 + this.half.getOrElse(0f)),
      full = Some(y3 + this.full.getOrElse(0f)))

  }

}

object Stats {
  implicit val format: OFormat[Stats] = Json.format[Stats]

  def empty: Stats = Stats(Data.empty, Data.empty, Data.empty)
}

object Data {
  implicit val format: OFormat[Data] = Json.format[Data]

  def empty: Data = Data(Some(0), Some(0), Some(0))


}

