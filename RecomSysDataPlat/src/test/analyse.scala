package com.ltw.app
package test

class analyse {
  val ratingText=sc.textFile("file:/root/data/ratings.dat")
  ratingText.first()
  val ratingRDD=ratingText.map(parseRating).cache()
  println("Total number of ratings: "+ratingRDD.count())
  println("Total number of movies rated: "+ratingRDD.map(_.product).distinct().count())
  println("Total number of users who rated movies: "+ratingRDD.map(_.user).distinct().count())
}
