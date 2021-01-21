import java.util.Dictionary

import main.{netflixDF, spark}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object Utils {
  def divideCommas(column: String,df: DataFrame) : DataFrame = {
    df.foreach(row => {
      if(row.getAs[String](column).contains(",")){
        val columnToReinsert = row.getAs[String](column).split(",")
        columnToReinsert.foreach(el => {

          val columnVal = List.empty[String]
          df.printSchema()
          df.show(20)
          df.columns.foreach(col => {
            if(col != column){
              columnVal :+ row.getAs[String](col)
            }else{
              columnVal :+ el
            }
          })

          val schema = StructType(
            df.columns.map(fieldName => StructField(fieldName, StringType,true))
           )

          val lineToInsertRdd = spark.sparkContext.parallelize(columnVal)

          val row2ToInsertRdd : RDD[Row] = lineToInsertRdd.map(line =>
            Row.fromSeq(line.split(','))
          )
          //df.union(spark.createDataFrame(,schema))
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
