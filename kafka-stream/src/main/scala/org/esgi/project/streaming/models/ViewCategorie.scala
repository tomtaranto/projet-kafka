package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class View2Categories(
                  categorie1_count: Float,
                  categorie2_count: Float
                )

case class View3Categories(
                          categorie1_count: Float,
                          categorie2_count: Float,
                          categorie3_count: Float
                        )
case class ViewFinalCategories(
                            title: String,
                            categorie1_count: Float,
                            categorie2_count: Float,
                            categorie3_count: Float
                          )

case class ViewAggregate(
                                _id:Int,
                                title: String,
                                categorie1_count: Float,
                                categorie2_count: Float,
                                categorie3_count: Float
                              ) {
  def increment(_id:Int,movie: String,category: String) = {
    category match{
      case "start_only" => {
        this.copy(_id,movie, categorie1_count= this.categorie1_count + 1)

      }
      case "half" => {
        this.copy(_id,movie,categorie2_count= this.categorie2_count + 1)
      }
      case _ => {
        this.copy(_id = _id, title= movie,categorie3_count= this.categorie3_count + 1)
      }
    }

  }

}

object View2Categories {
  implicit val format: OFormat[View2Categories] = Json.format[View2Categories]
}

object View3Categories {
  implicit val format: OFormat[View3Categories] = Json.format[View3Categories]
}
object ViewFinalCategories {
  implicit val format: OFormat[ViewFinalCategories] = Json.format[ViewFinalCategories]
}


object ViewAggregate {
  implicit val format: OFormat[ViewAggregate] = Json.format[ViewAggregate]

  def empty: ViewAggregate = ViewAggregate(0,"",0, 0, 0)
}
