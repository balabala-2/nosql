package spark

import Utils.Redis
import grpc.grpc_profile.{I2I_als, RecommendMovies, U2I, UserEmb}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object Embedding {

  val redisEndpoint = "localhost"
  val redisPort = 6379
  val spark: SparkSession = SparkSession
    .builder
    .config(
      new SparkConf()
        .setMaster("local")
        .setAppName("ctrModel")
        .set("spark.submit.deployMode", "client"))
    .getOrCreate()

  //根目录
  val root_path = "C://Users//0021//Desktop//recommend//src//main//resources//";
  //电影信息F:
  val movie_path: String = root_path + "original_data//movies.csv";
  //用户评分信息
  val rating64_path: String = root_path + "data_split//rating64.csv";
  val rating64_movie_path: String = root_path + "data_split//rating64_movie.csv";
  val ratingAvgRating: String = root_path + "data//ratingAvgRating.csv";
  val userModelPath: String = root_path + "model//userEmbModel"
  val alsModelPath: String = root_path + "model//alsModel"

  /**
   * |userId|movieIdStr    |
   * |100010|1409 605 785 18 104 1047 1061 733 6 32 36 1210 608    |
   * @return
   */
  def getRating: RDD[Seq[String]] = {
    var ratings = spark.read.format("csv").option("header", "true").load(ratingAvgRating)

    //sort by average score
    val sort: UserDefinedFunction = udf((rows: Seq[Row]) => {
      rows.map { case Row(movieId: String, avgRating: String) => (movieId, avgRating) }
        .sortBy { case (movieId, avgRating) => avgRating }
        .map { case (movieId, avgRating) => movieId }
    })

    ratings = ratings
      .where(col("rating") >= 3.5)
      .groupBy("userId")
      .agg(sort(collect_list(struct("movieId", "avgRating"))) as "movieIds")
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))
    //    ratings.select("userId", "movieIdStr").where(col("userId")<=51).show(50)
    ratings.select("movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)
  }

  /**
   * save word2vec model to the file
   *
   * @param embLength
   * @param filepath
   */
  def item2Vec(embLength: Int, filepath: String): Unit = {
    val ratings = getRating
    val word2vec = new Word2Vec()
      .setVectorSize(embLength)
      .setWindowSize(5)
      .setNumIterations(5)

    val model = word2vec.fit(ratings)

    model.save(spark.sparkContext, filepath)
  }

  /**
   * save als model to file
   *
   * @param filepath
   */
  def als(filepath: String): Unit = {
    val ratings = spark.read.format("csv").option("header", "true").load(ratingAvgRating)
      .where(col("rating") >= 3.5)
      .rdd.map({ case Row(userId, movieId, rating, score) =>
      Rating(userId.toString.toInt, movieId.toString.toInt, rating.toString.toFloat)
    })

    val rank = 20
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    model.save(spark.sparkContext, filepath)
  }

  /**
   * load user and the top n movies he/she like to redis
   */
  def loadAlsU2I(): Unit ={
    val model = MatrixFactorizationModel.load(spark.sparkContext, alsModelPath)

    model.recommendProductsForUsers(50).flatMap(x => {
      val user = x._1.toString
      val ratings = x._2
      val u2i = U2I.newBuilder().setUserId(x._1)
      for (rating <- ratings) {
        u2i.addMovies(RecommendMovies.newBuilder()
          .setMovieIds(rating.product)
          .setRating(rating.rating.toFloat).build())
      }
      Redis.getInstance().set(("U2I" + user).getBytes(), u2i.build().toByteArray)
      user
    }).saveAsTextFile(root_path + "//U2IData")
  }

  /**
   * load user embedding to redis
   */
  def loadUserEmb(): Unit ={
    val model = MatrixFactorizationModel.load(spark.sparkContext, alsModelPath)
    val features = model.userFeatures
    features.map(x => {
      val builder = UserEmb.newBuilder()
      builder.setUserID(x._1)
      val values = x._2
      for(value <- values){
        builder.addEmb(value.toFloat)
      }
      val emb = builder.build()
      Redis.getInstance().set(("UserEmb" + x._1).getBytes(), emb.toByteArray)
      x._1 + " " + x._2.mkString("Array(", ", ", ")")
    }).saveAsTextFile(root_path + "//userEmb")
  }


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.10.1")
    Logger.getLogger("org").setLevel(Level.ERROR)
//    loadAlsU2I()
    loadUserEmb()
    //    item2Vec(15, userModelPath)
    //    als(alsModelPath)
  }
}
