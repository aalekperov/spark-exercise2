package org.schema

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object SchemaObject {

  val userIdColumn = "user id"
  val itemIdColumn = "item id"
  val ratingColumn = "rating"
  val timestampColumn = "timestamp"
  val movieIdColumn = "movie id"
  val movieTitleColumn = "movie title"
  val histAllColumn = "hist_all"
  val ratingListColumn = "rating list"
  val countColumn = "count"

  val ratingSchema: StructType = StructType(Array(
    StructField(userIdColumn, StringType),
    StructField(itemIdColumn, LongType),
    StructField(ratingColumn, LongType),
    StructField(timestampColumn, StringType)))

  val movieSchema: StructType = StructType(Array(
    StructField(movieIdColumn, LongType),
    StructField(movieTitleColumn, StringType),
    StructField("release date", StringType),
    StructField("video release date", StringType),
    StructField("IMDb URL", StringType),
    StructField("unknown", StringType),
    StructField("Action", StringType),
    StructField("Adventure", StringType),
    StructField("Animation", StringType),
    StructField("Children's", StringType),
    StructField("Comedy", StringType),
    StructField("Crime", StringType),
    StructField("Documentary", StringType),
    StructField("Drama", StringType),
    StructField("Fantasy", StringType),
    StructField("Film-Noir", StringType),
    StructField("Horror", StringType),
    StructField("Musical", StringType),
    StructField("Mystery", StringType),
    StructField("Romance", StringType),
    StructField("Sci-Fi", StringType),
    StructField("Thriller", StringType),
    StructField("War", StringType),
    StructField("Western", StringType)
  ))


}
