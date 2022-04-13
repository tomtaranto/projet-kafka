package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class View2Categories(
                  _id: Int,
                  title: String,
                  categorie1_count: Float,
                  categorie2_count: Float
                )

case class View3Categories(
                          _id: Int,
                          title: String,
                          categorie1_count: Float,
                          categorie2_count: Float,
                          categorie3_count: Float
                        )

object View2Categories {
  implicit val format: OFormat[View2Categories] = Json.format[View2Categories]
}

object View3Categories {
  implicit val format: OFormat[View3Categories] = Json.format[View3Categories]
}