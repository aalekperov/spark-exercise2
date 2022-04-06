package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data.DataManage
import org.schema.SchemaObject._

object MainApp {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Main Application")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", 10)

  def main(args: Array[String]): Unit = {
    val ratingDataPath = args(0)
    val moviesDataPath = args(1)
    val resultJson = args(2)
    val movieId = 32

    val dm = new DataManage(spark)

    val ratingData = dm.readData(ratingDataPath, "\t", ratingSchema).cache()
    val movieData = dm.readData(moviesDataPath, "|", movieSchema).cache()

    val joinedData: DataFrame = dm.joinedDf(ratingData, movieData)

    val movieRatingDataByID = dm.ratingById(joinedData, movieData, movieId)
    val allMovieRatingData = dm.allRating(joinedData, movieData)

    val resultRatingData = movieRatingDataByID union allMovieRatingData

    val json = dm.getJsonData(resultRatingData)
    dm.writeToFile(resultJson, json)

  }

}
