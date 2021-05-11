package com.ltw.app
package test

import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

class ParseUtils {

  case class Movie(movieId:Int, title:String, genres:Seq[String])
  case class User(userId:Int, gender:String, age:Int, occupation:Int, zip:String)

  //Define parse function
  def parseMovie(str: String): Movie = {
    val fields=str.split("::")
    assert(fields.size==3)
    Movie(fields(0).toInt, fields(1).toString, Seq(fields(2)))
  }
  def parseUser(str: String): User = {
    val fields=str.split("::")
    assert(fields.size==5)
    User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt, fields(4).toString)
  }
  def parseRating(str: String): Rating = {
    val fields=str.split("::")
    assert(fields.size==4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }
}
