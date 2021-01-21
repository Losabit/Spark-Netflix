import java.util.Dictionary

import main.{netflixDF, spark}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Utils {
  def compare_type(df : DataFrame) : List[(String, Long)] = {
    var result : List[(String, Long)] = List()
    val typesDF = netflixDF.select(netflixDF("type")).distinct
    for(i <- 0 to typesDF.count.asInstanceOf[Int] - 1) {
      val typeToSearch = typesDF.collectAsList().get(i).getString(0)
      result = result :+ ((typeToSearch,  df.where("type = '" + typeToSearch + "'").count()))
    }
    result
  }

  def divideCommas(column: String,df: DataFrame) : DataFrame = {
    df.foreach(row => {
      if(row.getAs[String](column).contains(",")){
        val columnToReinsert = row.getAs[String](column).split(",")
        columnToReinsert.foreach(el => {

          val columnVal = List.empty[String]

          df.columns.foreach(col => {
            if(col != column){
              columnVal :+ row.getAs[String](col)
            }else{
              columnVal :+ row.getAs[String](el)
            }
          })

          val schema = StructType(
            df.columns.map(fieldName => StructField(fieldName, StringType,true))
           )

          val lineToInsertRdd = spark.sparkContext.parallelize(columnVal)
          df.union(spark.createDataFrame(lineToInsertRdd,schema))
        })
      }
    })
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
