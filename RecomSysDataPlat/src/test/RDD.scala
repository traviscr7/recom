package com.ltw.app
package test
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}
class RDD {
  val ratingDF=ratingRDD.toDF();
  val movieDF=sc.textFile("file:/root/data/movies.dat").map(parseMovie).toDF()
  val userDF=sc.textFile("file:/root/data/users.dat").map(parseUser).toDF()
  ratingDF.printSchema()
  movieDF.printSchema()
  userDF.printSchema()
  ratingDF.registerTempTable("ratings")
  movieDF.registerTempTable("movies")
  userDF.registerTempTable("users")

  val result=spark.sql("""select title,rmax,rmin,ucnt from (select product, max(rating) as rmax, min(rating) as rmin, count(distinct user) as ucnt
from ratings group by product) ratingsCNT join movies on product=movieId order by ucnt desc""")
  result.show()

  val mostActiveUser=spark.sql("""select user, count(*) as cnt
from ratings group by user order by cnt desc limit 10""")
  mostActiveUser.show()
  val result=spark.sql("""select distinct title, rating
from ratings join movies on movieId=product
where user=4169 and rating>4""")
  result.show()
}
