package org.data

import org.apache.commons.lang3.Validate
import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.schema.SchemaObject._

import java.io.{File, PrintWriter}

class DataManage(spark: SparkSession) {
  import spark.implicits._

  /**
   * Read data from csv files using delimiter.
   *
   * @param dataPath  File path
   * @param delimiter Delimiter that used in csv
   * @param schema    Schema of data
   * @return DataFrame
   * @throws IllegalArgumentException if the file path is empty
   * @throws NullPointerException     if the path is null
   */
  def readData(dataPath: String, delimiter: String, schema: StructType): DataFrame = {
    Validate.notEmpty(dataPath, "Data path cannot be empty.")
    spark.read.format("csv")
      .option("delimiter", delimiter)
      .schema(schema)
      .load(dataPath)
  }

  /**
   * Join rating data and movie data frames using common column.
   *
   * @param dataFrame1 Data with ratings.
   * @param dataFrame2 Data with movies.
   * @return DataFrame with columns movieIdColumn|movieTitleColumn|ratingColumn
   */
  def joinedDf(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    dataFrame1
      .join(dataFrame2, dataFrame1(itemIdColumn) === dataFrame2(movieIdColumn))
      .select(col(movieIdColumn), col(movieTitleColumn), col(ratingColumn))
  }

  /**
   * Rating dataframe contained only one column rating with all possible values.
   */
  private val rating = Seq(1, 2, 3, 4, 5).toDF(ratingColumn)

  /**
   * Movies dataframe with rating count 0. That's needed to restore rating with 0.
   * @param data  Movie dataFrame
   * @return      Dataframe with rating count equal to 0 with columns movie movie id|title|rating|count
   */
  private def moviesWithZeroRating(data: DataFrame): DataFrame = {
    data
      .select(col(movieIdColumn), col(movieTitleColumn))
      .crossJoin(rating)
      .withColumn(countColumn, lit(0))
  }

  /**
   * Dataset with count of rating with existing values and restored 0 value for given movie id.
   * @param data        Joined input Dataframe with movieIdColumn|movieTitleColumn|ratingColumn.
   * @param id          Movie id.
   * @param movieData   Movie dataframe.
   * @return            Dataset for movie with given id with rating list: movie title|rating list
   */
  def ratingById(data: DataFrame, movieData: DataFrame, id: Long): Dataset[(String, Seq[Long])] = {
    Validate.notNull(data)
    Validate.notNull(movieData)
    Validate.notNull(id, "Id should be defined as not null")
    Validate.isTrue(id <= data.select(col(movieIdColumn)).distinct().count(), "Id should not be more than movie size")
    val countDfById= data
      .where(col(movieIdColumn).equalTo(id))
      .groupBy(col(movieTitleColumn), col(ratingColumn))
      .count()
      .orderBy(col(ratingColumn))

    val zeroRatingMovies  = moviesWithZeroRating(movieData)
      .where(col(movieIdColumn).equalTo(id))
      .drop(col(movieIdColumn))

    zeroRatingMovies
      .join(countDfById, Seq(ratingColumn), "anti")
      .select(col(movieTitleColumn), col(ratingColumn), col("count"))
      .union(countDfById)
      .orderBy(ratingColumn)
      .groupBy(col(movieTitleColumn))
      .agg(collect_list(col("count")).as(ratingListColumn))
      .as[(String, Seq[Long])]
  }

  /**
   * Dataset with count of rating with existing values and restored 0 value.
   * @param data        Joined input Dataframe with movieIdColumn|movieTitleColumn|ratingColumn.
   * @param movieData   Movie dataframe.
   * @return            Dataset all movies rating list: movie title|rating list.
   */
  def allRating(data: DataFrame, movieData: DataFrame): Dataset[(String, Seq[Long])] = {
    Validate.notNull(data)
    Validate.notNull(movieData)
    val countDfById= data
      .groupBy(col(ratingColumn))
      .count()
      .orderBy(col(ratingColumn))
      .withColumn(movieTitleColumn, lit(histAllColumn))
      .select(col(movieTitleColumn), col(ratingColumn), col(countColumn))

    val zeroRatingMovies = rating
      .withColumn(movieTitleColumn, lit(histAllColumn))
      .withColumn(countColumn, lit(0))

    zeroRatingMovies
      .join(countDfById, Seq(ratingColumn), "anti")
      .select(col(movieTitleColumn), col(ratingColumn), col(countColumn))
      .union(countDfById)
      .orderBy(ratingColumn)
      .groupBy(col(movieTitleColumn))
      .agg(collect_list(col(countColumn)).as(ratingListColumn))
      .as[(String, Seq[Long])]
  }

  /**
   * Transformer from data to json.
   * @param data  Data afterrating calculation.
   * @return      String with Json getting from data.
   */
  def getJsonData(data: Dataset[(String, Seq[Long])]) : String = {
    Validate.notNull(data)
    val jsonMap: Map[String, Seq[Long]] = data.collect().toMap
    pretty(render(jsonMap))
  }


  /**
   * Create JSON file
   * @param filePath      Output file path.
   * @param dataToWrite   JSON String data.
   */
  def writeToFile(filePath: String, dataToWrite: String): Unit = {
    Validate.notEmpty(filePath)
    Validate.notEmpty(dataToWrite)
    val file = new File(filePath )
    val pw = new PrintWriter(file)
    pw.write(dataToWrite)
    pw.close()
  }

}