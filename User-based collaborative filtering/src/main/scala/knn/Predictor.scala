package knn

import knn.Predictor.similarity_by_cosine
import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import similarity.Predictor.{end_sim_pred, end_suv, global_ave_rating, mae_cosine, multiplication, normalize, normalized, normalized_item_grouped_cache, pair_in_test, r_ui_for_similarity, scale_calculation, start, start_suv, test, time_sim_pred, time_sum_suv, train, user_average_rating, user_grouped_similarity_cache, userid}
import similarity.Rating

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new Conf(args)
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
   assert(test.count == 20000, "Invalid test data")

  // ****************************************************************************************************************
  // My code starts here
  // -----------------------------------------Knn with varying k-----------------------------------------------------
  // Normalize the ratings
    val user_grouped = train.groupBy(t => t.user)
    val user_average_rating = user_grouped.map( l => l._1 -> {
      val count_each_user = l._2.size
      var sum_user:Double = 0.0
      l._2.foreach(sum_user += _.rating)
      sum_user/count_each_user.toDouble
    }).collectAsMap()
    val normalized= train.map(normalize)

    // calculate the denominator (r_prime) for each user and normalize the rating
      val normalized_user_grouped = normalized.groupBy(t => t.user)
      val r_prime = normalized_user_grouped.map(l => l._1 -> {
        l._2.map(x => x.rating * x.rating).sum
      }).collectAsMap()

      val r_ui_for_similarity = normalized.map(l => {
        Rating(l.user,l.item, l.rating/math.sqrt(r_prime(l.user)))
      })

  // Preparation for similarity
  val user_grouped_similarity = r_ui_for_similarity.groupBy(t => t.user)
  var multiplication = List[Double]() // Record the number of multiplications
  val user_grouped_similarity_cache = user_grouped_similarity.cache
  val pair_in_test = test.map(x => (x.user,x.item)).collect.toSet

  // Preparation for prediction
  val global_ave_rating = train.map(t => t.rating).mean // If there is no such user, take the global average rating directly.
  val items_in_test = test.map(t => t.item).collect.toSet
  val normalized_item_grouped = normalized.groupBy(t => t.item)
    .filter(m => items_in_test.contains(m._1)) // Grouped by item and filter the items not presented in the test set
  val normalized_item_grouped_cache = normalized_item_grouped.cache
  val userid = train.map(t => t.user).cache


  val similarity_by_cosine = user_grouped_similarity_cache.cartesian(user_grouped_similarity_cache) // Get all pairs of different users and their ratings
            //.filter(l => (l._1._1 != l._2._1)) // Filter the trivial pairs
            .map(x => {
              val u = x._1._2.map(l => l.item -> l.rating).toMap
              val v = x._2._2.map(l => l.item -> l.rating).toMap
              val intesection = u.keySet.intersect(v.keySet)
              var sim = 0.0
              intesection.foreach(k => (sim += u(k) * v(k)))
              //val sim = intesection.toArray.map(k => u(k) * v(k)).sum
              ((x._1._1, x._2._1), sim)
              }).collectAsMap

  val neigoubours_desc_order = similarity_by_cosine
                              //.map(t => (t._1, t._2, similarity_by_cosine.getOrElse((if(t._1 < t._2) (t._1, t._2) else (t._2, t._1)),0.0)))
                              .groupBy(l => l._1._1)
                              .map(m => (m._1, m._2.toArray.sortBy(-_._2)))//.map(m => (m._1, m._2.toArray.sortBy(-_._3))) //Sort the similarity in descending order and map the user to its neighbours
                              .map(m => (m._1, m._2.map(_._1._2))) // (userid, Array of neighbours arranged in an descending order based on similarity)

  // Calculate the MAE by varying the number of neighbours in the model
  val k_sets = Array(943,800,400,200,100,50,30,10)
  var k_mae:Map[Int,Double] = Map()
  var Storage_K:Map[Int,Long] = Map()
  var i = 0
  //val start = System.nanoTime()
  for (k <- k_sets){
      println("We are in the loop", k)
      val k_neighbours = neigoubours_desc_order.map(l => (l._1, l._2.take(k).toSet))
      println("After taking k")
      val weighted_rating_cosine = userid.cartesian(normalized_item_grouped_cache) // ((userid,itemid),weighted_rating)
        .filter(x => pair_in_test.contains((x._1, x._2._1)))
        .map(x => {
          var denominator: Double = 0.0
          var nominator: Double = 0.0
          val u = x._1
          x._2._2.foreach( y => {
            val v = y.user
            if (k_neighbours(x._1).contains(v)) {
                val sim = similarity_by_cosine.getOrElse((if(u < v) (u, v) else (v, u)),0.0)
                nominator = nominator + y.rating * sim
                denominator = denominator + math.abs(sim)
            }
          })
         // println("finish on", u,x._2._1)
          if (math.abs(denominator) > 0.0001) ((u,x._2._1), nominator/denominator)
          else ((u,x._2._1), 0.0)
        }).collectAsMap
      println("After calculate weighted rating")

      val mae_cosine = test.map(l => {
        val r_u = user_average_rating.getOrElse(l.user, global_ave_rating)
        val r_i = weighted_rating_cosine.getOrElse((l.user, l.item), 0.0)
        val p_ui = r_u + r_i * scale_calculation(r_u + r_i, r_u)
        (l.rating - p_ui).abs
      }).sum/test.count
     println("After MAE")
     k_mae += (k -> mae_cosine)
     Storage_K += (k -> k_neighbours.map(a => a._2.size).sum * (8 + 4 + 4))
  }
 // val end = System.nanoTime()

  //println("Time used", (end - start)/1e9)
  val mae_baseline = 0.7669
  val LowestKWithBetterMaeThanBaseline = k_mae.filter(x => x._2 < mae_baseline).keys.min
  val LowestKMaeMinusBaselineMae = k_mae(LowestKWithBetterMaeThanBaseline) - mae_baseline

 // Storage_K = k_sets.map(i => (i -> i * userid.count * (8 + 4 + 4))).toMap
  // ------------------------Calculate minimum number of bytes required ------------------------------------------------

   val RAM = 8589934592.0.toLong
   val MaximumNumberOfUsersThatCanFitInRam = (RAM.toDouble/(8 + 4 + 4)/LowestKWithBetterMaeThanBaseline).toLong




  //  val neigoubours_desc_order = userid.cartesian(userid) // A map where the values is the userids organized in an descending order based on similarity
  //                               .filter(l => (l._1 != l._2))
  //                               .groupBy(l => l._1)
  //                               .foreach(m => m._2.map(k => (k, similarity_by_cosine.getOrElse((if(k._1 < k._2) (k._1, k._2) else (k._2, k._1)),0.0))))
  //                               .foreach


  //  -----------------------------------Functions-------------------------------------------------------------

  def normalize(rating: Rating): Rating =
  {
    val r = rating.rating
    val u = rating.user
    val u_aver = user_average_rating(u)
    val scale = scale_calculation(r,u_aver)
    Rating(rating.user,rating.item,(rating.rating - u_aver)/scale)
  }

  def scale_calculation(r_ui: Double, r_u: Double):Double = {
    if (r_ui > r_u) 5 - r_u
    else if (r_ui < r_u) r_u - 1
    else 1
  }

  def std(time: List[Double]):Double = {
    val avg = time.sum/time.size
    scala.math.sqrt(time.map(x => scala.math.pow((x - avg),2)).sum/(time.size - 1))
  }


  // My code ends here
  // ****************************************************************************************************************

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> k_mae(10), // Datatype of answer: Double
            "MaeForK=30" -> k_mae(30), // Datatype of answer: Double
            "MaeForK=50" -> k_mae(50), // Datatype of answer: Double
            "MaeForK=100" -> k_mae(100), // Datatype of answer: Double
            "MaeForK=200" -> k_mae(200), // Datatype of answer: Double
            "MaeForK=400" -> k_mae(400), // Datatype of answer: Double
            "MaeForK=800" -> k_mae(800), // Datatype of answer: Double
            "MaeForK=943" -> k_mae(943), // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> LowestKWithBetterMaeThanBaseline, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> LowestKMaeMinusBaselineMae // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> Storage_K(10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> Storage_K(30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> Storage_K(50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> Storage_K(100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> Storage_K(200), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> Storage_K(400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> Storage_K(800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> Storage_K(943) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> RAM, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> MaximumNumberOfUsersThatCanFitInRam // Datatype of answer: Int
          )

          // Answer the Question 3.2.4 exclusively on the report.
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
