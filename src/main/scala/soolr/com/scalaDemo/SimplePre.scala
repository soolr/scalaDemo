package soolr.com.scalaDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.jblas.DoubleMatrix

object SimplePre {
    def main(args : Array[String]) {
      
    val dataFile = "F:/ml-100k/u.data"   
    val moviesFile = "F:/ml-100k/u.item" 
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var rawData = sc.textFile(dataFile)
    var rawRatings = rawData.map (_.split("\t").take(3))
    var ratings = rawRatings.map { case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble) }
    
    var movies = sc.textFile(moviesFile)
    var titles = movies.map( line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
//    var titless = movies.map( line => line.split("\\|").take(2)).map { case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble) }
    
    var model = ALS.train(ratings, 50, 10, 0.01)//因子个数,迭代次数,正则化值
    var predictedRating = model.predict(789, 123)//用户789对电影123的评分预测
    var userId = 789
    var K = 10
    var topKRecs = model.recommendProducts(userId, K)//用户789的预测评分前十的电影和评分
    println( predictedRating )
    println( topKRecs.mkString("\n") )
    
    var moviesForUser =  ratings.keyBy(_.user).lookup(789)//用户789评价的电影
    println( moviesForUser.size )
    
    var aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))
    
    moviesForUser.sortBy( -_.rating ).take(10).map( rating => (titles(rating.product),rating.rating)).foreach(println)//用户评价前十的电影
    println("---------------------------")
    topKRecs.map(rating => (titles(rating.product),rating.rating)).foreach(println)//预测用户评分前十的电影
    println("---------------------------")
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    //求各个物品的余弦相似度
    val sims = model.productFeatures.map{
      case (id,factor) => 
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector,itemVector)
        (id,sim)
    }
    //对物品按照相似度排序,取出与567最相似的前10个物品(已定义过K=10)
    val sortedSims = sims.top(K) (Ordering.by [(Int,Double),Double]{
      case (id,similarity) => similarity
    })
    val sortedSims2 = sims.top(K+1)(Ordering.by[(Int,Double),Double]{
      case (id,similarity) => similarity
    })
    println(itemVector)
    println(cosineSimilarity(itemVector, itemVector))
    println(sortedSims.mkString("\n"))
    println(titles(itemId))
    println("---------------------------")
    println(sortedSims2.slice(1, 11).map{
      case(id,sim) => (titles(id),sim)
    }.mkString("\n"))    
    }
    
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
    }

}