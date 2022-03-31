package org.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.schema.SchemaObject._
import org.test.SparkSessionTestWrapper

class DataManageTest extends AnyFunSuite with SparkSessionTestWrapper{

  val ratingDataPath = "src/test/resources/u.data"
  val moviesDataPath = "src/test/resources/u.item"

  val dataManage = new DataManage(spark)
  val ratingData: DataFrame = dataManage.readData(ratingDataPath, "\t", ratingSchema)
  val movieData: DataFrame = dataManage.readData(moviesDataPath, "|", movieSchema)
  val joinedData: DataFrame = dataManage.joinedDf(ratingData, movieData)

  test("read file - correct") {
    assert(ratingData.count() == 30, "Check that row count is 30")
    assert(ratingData.columns.length == 4, "Check that only 4 columns in data")
    ratingSchema.foreach(field =>
      assert(ratingData.columns.contains(field.name), "Check that all columns from schema exist in data")
    )

    assert(ratingData.schema(userIdColumn).dataType == StringType, s"Check that $userIdColumn data type is StringType")
    assert(ratingData.schema(itemIdColumn).dataType == LongType, s"Check that $itemIdColumn data type is LongType")
    assert(ratingData.schema(ratingColumn).dataType == LongType, s"Check that $ratingColumn data type is LongType")
    assert(ratingData.schema(timestampColumn).dataType == StringType, s"Check that $timestampColumn data type is StringType")
  }

  test("read file - null path") {
    intercept[NullPointerException] {
      dataManage.readData(null, "\t", ratingSchema)
    }
  }

  test("read file - empty path ") {
    intercept[IllegalArgumentException] {
      dataManage.readData("", "\t", ratingSchema)
    }
  }

  test("joinedDf - correct") {
    assert(joinedData.count() == 30, "Check that after joining the data size is 30 as before")
    assert(joinedData.columns.contains(movieIdColumn), s"Check that data is contained $movieIdColumn")
    assert(joinedData.columns.contains(movieTitleColumn), s"Check that data is contained $movieTitleColumn")
    assert(joinedData.columns.contains(ratingColumn), s"Check that data is contained $ratingColumn")
  }

  test("ratingById - correct") {
    val movieId = 5
    val movieRatingDataByID = dataManage.ratingById(joinedData, movieData, movieId)

    assert(movieRatingDataByID.count() == 1, "Check that data size ia equal to 1")
    assert(movieRatingDataByID.columns.length == 2, "Check that column size is equal 2")
    assert(movieRatingDataByID.columns.contains(movieTitleColumn), s"Check that data is contained $movieTitleColumn")
    assert(movieRatingDataByID.columns.contains(ratingListColumn), s"Check that data is contained $ratingListColumn")
    assert(movieRatingDataByID.collect()(0)._1 == "Copycat (1995)", "Check that movie name is same as expected")
    assert(movieRatingDataByID.collect()(0)._2 == List(0,1,1,2,4), "Check that rating list is same as expected")
  }

  test("ratingById - incorrect id") {
    val id = 7
    intercept[IllegalArgumentException] {
      dataManage.ratingById(joinedData, movieData, id)
    }
  }

  test("ratingById - null joined data") {
    val id = 4
    intercept[NullPointerException] {
      dataManage.ratingById(null, movieData, id)
    }
  }

  test("ratingById - null movie data") {
    val id = 4
    intercept[NullPointerException] {
      dataManage.ratingById(joinedData, null, id)
    }
  }


  test("allRating - correct") {
    val allRatingData = dataManage.allRating(joinedData, movieData)

    assert(allRatingData.count() == 1, "Check that data size ia equal to 1")
    assert(allRatingData.columns.length == 2, "Check that column size is equal 2")
    assert(allRatingData.columns.contains(movieTitleColumn), s"Check that data is contained $movieTitleColumn")
    assert(allRatingData.columns.contains(ratingListColumn), s"Check that data is contained $ratingListColumn")
    assert(allRatingData.collect()(0)._1 == "hist_all", "Check that movie name is same as expected")
    assert(allRatingData.collect()(0)._2 == List(0,7,9,5,9), "Check that rating list is same as expected")
  }

  test("allRating - null movie data") {
    intercept[NullPointerException] {
      dataManage.allRating(joinedData, null)
    }
  }

  test("allRating - null joined data") {
    intercept[NullPointerException] {
      dataManage.allRating(null, movieData)
    }
  }

}
