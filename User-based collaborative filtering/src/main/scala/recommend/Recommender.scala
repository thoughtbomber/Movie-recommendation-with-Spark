package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)
case class myRating(item: Int, name: String, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data())
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal())
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  val personal = personalFile.map(l => {
    val cols = l.split(",").map(_.trim)
    myRating(cols(0).toInt, cols(1), cols(2).toDouble)
  })
  val my_rating = personal.filter(b => b.rating > 0.0)
  assert(personalFile.count == 1682, "Invalid personal data")

  // ****************************************************************************************************************
  // My code starts here
  // ----------------------------------  Add my rating into the main dataset ----------------------------------------
  val rating_to_add = my_rating.map(l => Rating(944, l.item, l.rating))
  val train = data.union(rating_to_add)

  // ----------------------------------  Start Training the model with KNN ------------------------------------------
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
  val pair_in_test = personal.map(x => (944,x.item)).collect.toSet

  // Preparation for prediction
  val global_ave_rating = train.map(t => t.rating).mean // If there is no such user, take the global average rating directly.
  val items_in_test = personal.map(t => t.item).collect.toSet
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
                  .groupBy(l => l._1._1)
                  .map(m => (m._1, m._2.toArray.sortBy(-_._2)))//.map(m => (m._1, m._2.toArray.sortBy(-_._3))) //Sort the similarity in descending order and map the user to its neighbours
                  .map(m => (m._1, m._2.map(_._1._2))) // (userid, Array of neighbours arranged in an descending order based on similarity)

  // Calculate the MAE by varying the number of neighbours in the model
  val k_sets = Array(30,300)
  var res:Map[Int,Array[myRating]] = Map()

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

        val my_average_rating = my_rating.map(l => l.rating).mean
        val my_prediction = personal.filter(l => l.rating < 0.5)  //Filter to get the movies I have not watched yet.
          .map(m => {
              val r_i = weighted_rating_cosine.getOrElse((944, m.item), 0.0)
               val my_pred = my_average_rating + r_i * scale_calculation(my_average_rating + r_i, my_average_rating)
              myRating(m.item, m.name, my_pred)
        })
        println("After prediction")
        res += (k -> my_prediction.sortBy(l => l.rating, ascending = false).take(5))
        //my_prediction.sortBy(l => (l.rating, l.item), ascending = false).take(1000).foreach(println)
  }


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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" ->
              List[Any](
                List(res(30)(0).item, res(30)(0).name, res(30)(0).rating), // Datatypes for answer: Int, String, Double
                List(res(30)(1).item, res(30)(1).name, res(30)(1).rating), // Representing: Movie Id, Movie Title, Predicted Rating
                List(res(30)(2).item, res(30)(2).name, res(30)(2).rating), // respectively
                List(res(30)(3).item, res(30)(3).name, res(30)(3).rating),
                List(res(30)(4).item, res(30)(4).name, res(30)(4).rating)
              ),

            "Top5WithK=300" ->
              List[Any](
                List(res(300)(0).item, res(300)(0).name, res(300)(0).rating), // Datatypes for answer: Int, String, Double
                List(res(300)(1).item, res(300)(1).name, res(300)(1).rating), // Representing: Movie Id, Movie Title, Predicted Rating
                List(res(300)(2).item, res(300)(2).name, res(300)(2).rating), // respectively
                List(res(300)(3).item, res(300)(3).name, res(300)(3).rating),
                List(res(300)(4).item, res(300)(4).name, res(300)(4).rating)
              )

            // Discuss the differences in rating depending on value of k in the report.
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
