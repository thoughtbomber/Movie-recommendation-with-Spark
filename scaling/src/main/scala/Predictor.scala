import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    println("Compute kNN on train data...")
    
    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    println("Compute predictions on test data...")


    // *************************************************************************************************
    // Calculate the user mean
    var start = System.nanoTime()
    var user_average_rating = DenseVector.zeros[Double](train.rows)
    var user_rating_count = DenseVector.zeros[Double](train.rows)
    for((k,v) <- train.activeIterator){
      val row = k._1
      user_average_rating(row) += v
      user_rating_count(row) += 1
    }
    user_rating_count.map(i => if (i != 0) i else 1.0) // Avoid division by zero
    user_average_rating = user_average_rating /:/ user_rating_count
    val global_ave_rating = sum(user_average_rating)/sum(user_rating_count)


    // Normalize the rating of each user
    def normalize(u: Int, r: Double): Double =
    {
      val u_aver = user_average_rating(u)
      val scale = scale_calculation(r, u_aver)
      (r - u_aver)/scale
    }
    def scale_calculation(r_ui: Double, r_u: Double):Double = {
      if (r_ui > r_u) 5 - r_u
      else if (r_ui < r_u) r_u - 1
      else 1
    }
    val builder = new CSCMatrix.Builder[Double](rows=train.rows, cols=train.cols)
    for((k,v) <- train.activeIterator){
        val row = k._1
        val col = k._2
        val value = normalize(row, v)
        builder.add(row, col, value)
        //println(row, col, value)
    }
    val normalized = builder.result() // To be used for calculation of the weighted score.
    // println("SUm of normalized rating is ", sum(normalized)) Correct

    // calculate the denominator (r_prime) for each user and normalize the rating
    var r_prime = DenseVector.zeros[Double](train.rows)
    for((k,v) <- normalized.activeIterator) {
      val row = k._1
      r_prime(row) += v * v
     }

    val builder_sim = new CSCMatrix.Builder[Double](rows=train.rows, cols=train.cols)
    for((k,v) <- normalized.activeIterator){
      val row = k._1
      val col = k._2
      val value = v/math.sqrt(r_prime(row))
      builder_sim.add(row, col, value)
    }
    val r_ui_for_similarity = builder_sim.result()
    
    // ----------------------------------------------------------------
    // Parallelize it from here
    val br = sc.broadcast(r_ui_for_similarity)
    
    def topk(u: Int) = {
      val r_ui_broadcasted = br.value
      val vector = r_ui_broadcasted(u, 0 until r_ui_broadcasted.cols).t.toDenseVector
      val s_u = r_ui_for_similarity * vector
      s_u(u) = 0.0
      (u, argtopk(s_u, conf_k).map(v => (v, s_u(v))))
   }

    
     val topks = sc.parallelize(0 until train.rows).map(topk).collect()
     val knnBuilder = new CSCMatrix.Builder[Double](rows=train.rows, cols=train.rows)
     for (i <- topks){
        val u = i._1
        i._2.foreach( m => knnBuilder.add(u, m._1, m._2))
    }
    val knn = knnBuilder.result()
    var end = System.nanoTime()
    val time_knn = (end - start)/1e3
   
   // Calculate the weighted score
      

   def weighted_rating_cosine(user: Int, item: Int): Double = {
      var sum_of_weighted_sim: Double = 0.0
      var sum_of_sim: Double = 0.0
      for (v <- 0 until knn.cols) {
        if (math.abs(normalized(v, item)) > 0.00001) {
          sum_of_weighted_sim += normalized(v, item) * knn(user, v) // if similarity = 0 , knn = 0
          sum_of_sim += math.abs(knn(user, v))
        }
      }
      if (sum_of_sim > 0) sum_of_weighted_sim / sum_of_sim
      else 0.0
    }

    def predict(ui:(Int,Int)) = {
      val u = ui._1
      val i = ui._2
      var r_u = user_average_rating(u)
      if (r_u == 0) r_u = global_ave_rating
      val r_i = weighted_rating_cosine(u, i)
      val pred = r_u + r_i * scale_calculation(r_u + r_i, r_u)
      (u, i, pred)
    }

    start = System.nanoTime()
    val result_builder = new CSCMatrix.Builder[Double](rows = test.rows, cols = test.cols)
    val pred_parallel = sc.parallelize(test.activeKeysIterator.toList).map(predict).collect()
    val predBuilder = new CSCMatrix.Builder[Double](rows=test.rows, cols=test.cols)
    for (i <- pred_parallel){
        predBuilder.add(i._1, i._2, i._3)
    }
    val pred = predBuilder.result()
    val mae = sum(abs(pred-test))/test.activeSize
    end = System.nanoTime()
    val time_prediction = (end - start)/1e3

    println("The predicition has finished and the mae is ", mae)
        

    // My code ends here.
    // *************************************************************************************************

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
            "Q4.1.1" -> Map(
              "MaeForK=200" -> mae  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> time_knn  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> time_prediction // Datatype of answer: Double  
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}