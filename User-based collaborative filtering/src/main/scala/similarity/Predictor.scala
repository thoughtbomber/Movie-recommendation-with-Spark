package similarity

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkContext

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

  // ************************************************************************************************************
  // My code starts here
  // -----------------------Prediction based on cosine similarity -----------------------------------------------
  // Normalize the ratings
    val user_grouped = train.groupBy(t => t.user)
    val user_average_rating = user_grouped.map( l => l._1 -> {
        val count_each_user = l._2.size
        var sum_user:Double = 0.0
        l._2.foreach(sum_user += _.rating)
        sum_user/count_each_user.toDouble
    }).collectAsMap()
   // println(user_average_rating)
    val normalized= train.map(normalize)
    var res = 0.0
    for (i <- normalized){
      res += i.rating
    }
    println("SUm of normalized rating is ", res)




  // calculate the denominator (r_prime) for each user and normalize the rating
    val normalized_user_grouped = normalized.groupBy(t => t.user)
    val r_prime = normalized_user_grouped.map(l => l._1 -> {
      l._2.map(x => x.rating * x.rating).sum
    }).collectAsMap()
  //r_prime.foreach(println)
  //println("After looking for")
    val r_ui_for_similarity = normalized.map(l => {
      Rating(l.user,l.item, l.rating/math.sqrt(r_prime(l.user)))
    })

  // Calculate the cosine similarity


    // Preparation for similarity
    val user_grouped_similarity = r_ui_for_similarity.groupBy(t => t.user)
    val user_grouped_similarity_cache = user_grouped_similarity.cache
    val pair_in_test = test.map(x => (x.user,x.item)).collect.toSet

    // Preparation for prediction
    val global_ave_rating = train.map(t => t.rating).mean // If there is no such user, take the global average rating directly.
    val items_in_test = test.map(t => t.item).collect.toSet
    val normalized_item_grouped = normalized.groupBy(t => t.item)
      .filter(m => items_in_test.contains(m._1)) // Grouped by item and filter the items not presented in the test set
    val normalized_item_grouped_cache = normalized_item_grouped.cache
    val userid = train.map(t => t.user).cache

    // Variables for recording
    var time_sim_pred = List[Double]()
    var time_sim = List[Double]()
    var time_sum_suv = 0.0
    var start = 0.0
    var start_suv = 0.0
    var end_suv = 0.0
    var end_sim = 0.0
    var end_sim_pred = 0.0
    var mae_cosine = 0.0
    var memory = 0
    var similarity_by_cosine_size = 0
    var multiplication = List[Double]() // Record the number of multiplications

    for(loop_no <- 1 to 5){
        println("In loop", loop_no)
        start = System.nanoTime()
        val similarity_by_cosine = user_grouped_similarity_cache.cartesian(user_grouped_similarity_cache) // Get all pairs of different users and their ratings
                                  .filter(l => (l._1._1 <= l._2._1)) // Filter the trivial pairs
                                  .map(x => {
                                    start_suv = System.nanoTime()
                                    val u = x._1._2.map(l => l.item -> l.rating).toMap
                                    val v = x._2._2.map(l => l.item -> l.rating).toMap
                                    val intesection = u.keySet.intersect(v.keySet)
                                    var sim = 0.0
                                    intesection.foreach(k => (sim += u(k) * v(k)))
                                    //val sim = intesection.toArray.map(k => u(k) * v(k)).sum
                                    end_suv = System.nanoTime()
                                    time_sum_suv = time_sum_suv + (end_suv - start_suv)/1e3
                                    multiplication = intesection.size :: multiplication
                                    ((x._1._1, x._2._1), sim)
                                  }).collectAsMap

        end_sim = System.nanoTime()
        time_sim = (end_sim - start)/1e3::time_sim
        similarity_by_cosine_size = multiplication.filter(x => x > 0).size // If the intersection has no elements, no compucation is conducted
        var jieguo = 0.0
        for (i <- similarity_by_cosine){
          jieguo += i._2
        }
      println("SUm of similarity is ", jieguo)
      // Calculate the MAE for cosine similarity
      val weighted_rating_cosine = userid.cartesian(normalized_item_grouped_cache) // ((userid,itemid),weighted_rating)
      .filter(x => pair_in_test.contains((x._1,x._2._1)))
      .map(x => {
            var denominator: Double = 0.0
            var nominator: Double = 0.0
            val u = x._1
            x._2._2.foreach( y => {
                val v = y.user
                val sim = similarity_by_cosine.getOrElse((if(u < v) (u, v) else (v, u)),0.0)


                nominator = nominator + y.rating * sim
                denominator = denominator + math.abs(sim)
              if (u == 459 && x._2._1 == 934) {
                println("Result of 459 item 934",v, nominator, denominator)
              }
            })

          if (math.abs(denominator) > 0.0001) ((u,x._2._1), nominator/denominator)
          else ((u,x._2._1), 0.0)
        }).collectAsMap

        memory = similarity_by_cosine.filter(x => x._2 > 0.0).size * (4 + 4 + 8)


        val mae_cosine_inner = test.map(l => {
          val r_u = user_average_rating.getOrElse(l.user, global_ave_rating)

          val r_i = weighted_rating_cosine.getOrElse((l.user, l.item), 0.0)
          //println(l.user, l.item, r_i)
          val p_ui = r_u + r_i * scale_calculation(r_u + r_i, r_u)
          (l.rating - p_ui).abs
        }).sum/test.count
        end_sim_pred = System.nanoTime()
        time_sim_pred = (end_sim_pred - start)/1e3::time_sim_pred
        mae_cosine = mae_cosine_inner
        println("MAE of cosine", mae_cosine)
    }


  println("MAE cosine is", mae_cosine)
  val mae_baseline = 0.7669

  // -----------------------Prediction based on Jaccard similarity --------------------------------------------
  val all_normalized_ratings = normalized.map(t => t.rating).collect.toArray.sorted
  val threshold = all_normalized_ratings((normalized.count*0.60).toInt) // Get the threshold to determine whether the user like this movie

  val normalized_with_like = normalized.filter(t => t.rating > threshold).groupBy(t => t.user).cache

  val similarity_by_jaccard = normalized_with_like.cartesian(normalized_with_like) // Get all pairs of different users and their ratings
    .filter(l => (l._1._1 < l._2._1)) // Filter the trivial pairs
    .map(x => {
      val u = x._1._2.map(l => l.item).toSet
      val v = x._2._2.map(l => l.item).toSet
      val sim = jaccard(u,v)
      ((x._1._1, x._2._1), sim)
    }).collectAsMap


  val weighted_rating_jaccard = userid.cartesian(normalized_item_grouped_cache) // ((userid,itemid),weighted_rating)
    .filter(x => pair_in_test.contains((x._1,x._2._1)))
    .map(x => {
      var denominator: Double = 0.0
      var nominator: Double = 0.0
      val u = x._1
      x._2._2.foreach( y => {
        val v = y.user
        val sim = similarity_by_jaccard.getOrElse((if(u < v) (u, v) else (v, u)),0.0)
        nominator = nominator + y.rating * sim
        denominator = denominator + math.abs(sim)
      })
      if (math.abs(denominator) > 0.0001) ((u,x._2._1), nominator/denominator)
      else ((u,x._2._1), 0.0) // check reasonable
    }).collectAsMap

  val mae_jaccard = test.map(l => {
    val r_u = user_average_rating.getOrElse(l.user, global_ave_rating)
    val r_i = weighted_rating_jaccard.getOrElse((l.user, l.item), 0.0)
    val p_ui = r_u + r_i * scale_calculation(r_u + r_i, r_u)
    (l.rating - p_ui).abs
  }).sum/test.count

  println("MAE jaccard", mae_jaccard)


  //  -----------------------------------Functions-------------------------------------------------------------
  def jaccard(a: Set[Int], b: Set[Int]): Double = {
    val inter = a.intersect(b)
    inter.size.toDouble / (a.size + b.size - inter.size).toDouble
  }

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
  // ************************************************************************************************************

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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> mae_cosine, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (mae_cosine - mae_baseline) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> mae_jaccard, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (mae_jaccard - mae_cosine) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" ->  similarity_by_cosine_size// Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> multiplication.min,  // Datatype of answer: Double
              "max" -> multiplication.max, // Datatype of answer: Double
              "average" -> multiplication.sum/multiplication.size, // Datatype of answer: Double
              "stddev" -> std(multiplication) // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> memory// Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> time_sim_pred.min,  // Datatype of answer: Double
              "max" -> time_sim_pred.max, // Datatype of answer: Double
              "average" -> time_sim_pred.sum/time_sim_pred.size, // Datatype of answer: Double
              "stddev" -> std(time_sim_pred) // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> time_sim.min,  // Datatype of answer: Double
              "max" -> time_sim.max, // Datatype of answer: Double
              "average" -> time_sim.sum/time_sim.size, // Datatype of answer: Double
              "stddev" -> std(time_sim) // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> time_sum_suv/similarity_by_cosine_size, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> (time_sim.sum/time_sim.size)/(time_sim_pred.sum/time_sim_pred.size) // Datatype of answer: Double
          )
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
