package com.ltw.app
package test

class ALS {
  val splits=ratingRDD.randomSplit(Array(0.8,0.2), 0L)
  val trainingSet=splits(0).cache()
  val testSet=splits(1).cache()
  trainingSet.count()
  testSet.count()
  val model=(new ALS().setRank(20).setIterations(10).run(trainingSet))

  val recomForTopUser=model.recommendProducts(3033,10)
  val movieTitle=movieDF.map(array=>(array(0),array(1))).collectAsMap();//报错
  val recomResult=recomForTopUser.map(rating=>(movieTitle(rating.product),rating.rating)).foreach(println)

  val testUserProduct=testSet.map{
    case Rating(user,product,rating) => (user,product)
  }
  val testUserProductPredict=model.predict(testUserProduct)
  testUserProductPredict.take(10).mkString("\n")

  val testSetPair=testSet.map{
    case Rating(user,product,rating) => ((user,product),rating)
  }
  val predictionsPair=testUserProductPredict.map{
    case Rating(user,product,rating) => ((user,product),rating)
  }

  val joinTestPredict=testSetPair.join(predictionsPair)
  val mae=joinTestPredict.map{
    case ((user,product),(ratingT,ratingP)) =>
      val err=ratingT-ratingP
      Math.abs(err)
  }.mean()

}
