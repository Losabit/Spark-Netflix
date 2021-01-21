import main.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.::


object Utils {

  def divideCommas(column: String,df: DataFrame,dfColumns: Array[String]) : DataFrame = {
    df.foreach(row => {
      val columnToDivide = row.getAs[String](column)
      if(columnToDivide != null && columnToDivide.contains(",")){
        val columnToReinsert = columnToDivide.split(",")
        columnToReinsert.foreach(el => {

          var columnVal = Seq[String]()

          dfColumns.foreach(col => {
            if(col != column){
              columnVal = columnVal :+ row.getAs[String](col)
            }else{
              columnVal = columnVal :+ el
            }
          })

          import spark.implicits._
          var lineToInsert = columnVal.toDF(
            "show_id", "type", "title", "director", "cast", "country", "date_added",
              "release_year", "rating", "duration", "listed_in", "description"
          )
          df.union(lineToInsert)
        })
      }
    })
    df
  }

  def mostCountry(df : DataFrame): String = {
    var newDf = df.select("*").groupBy("country").count().orderBy(desc("count"))
    return  newDf.toString
  }

  def mostDirector(df : DataFrame)  : String = {
    var directorDf = df.select("*").where("director != '' ").groupBy("director").count().orderBy(desc("count"))
    return directorDf.first().getString(0)
  }
}
