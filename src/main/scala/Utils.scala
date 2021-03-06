import main.spark.implicits._
import main.{netflixDF, spark}
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{DataFrame, Dataset}


object Utils {
  def fieldToList(df: DataFrame, col: Int): Dataset[(String, String)] = {
    val dataset: Dataset[(String, List[String])] = netflixDF.map {
      row => {
        if (row != null && row.get(0) != null && row.get(col) != null)
          (row.getString(0), row.getString(col).split(',').toList)
        else
          ("-1",List())
      }
    }
    getAllFromList(dataset)
  }

  def getAllFromList(dataset: Dataset[(String,List[String])]) = {
    import spark.implicits._
    val datasetFinal = dataset.flatMap({
      case(key, value) => value.map(v => (key,v))
    })
    datasetFinal
  }

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

  def mostElem(df: Dataset[(String, String)], colName: String): String = {
    var newDf = renameColumn(df, colName)
    var finalCountry = newDf.select("*").groupBy(colName).count().orderBy(desc("count"))
    return finalCountry.first().getString(0)
  }
  def getOlder(df: Dataset[(String, String)], colName: String): String = {
    var newDf = renameColumn(df, colName)
    var older = newDf.select("*").where("release_year != 'United States' AND  id ='s4868'" ).orderBy(asc(colName))
   // older.show()
    return older.first().getString(1)
  }
  def getNew(df: Dataset[(String, String)], colName: String): String = {
    var newDf = renameColumn(df, colName)
    var older = newDf.select("*").where("release_year != 'United States' AND release_year > 2020" ).orderBy(asc(colName))
//    older.show()
    return older.first().getString(1)
  }


  def renameColumn(dataset: Dataset[(String, String)], column: String): DataFrame = {
    var dt = dataset.withColumnRenamed("_1", "id").withColumnRenamed("_2", column)
    dt
  }

  def getMostRatedType(dataFrame: DataFrame): String = {
    var df = dataFrame.select("rating", "type").groupBy("type").count().orderBy(desc("count")).limit(2)
    var finalDf = df.withColumnRenamed("count", "totalRating")
    return finalDf.first().getString(0)
  }
}
