import java.util.Dictionary
<<<<<<< HEAD
import main.netflixDF
=======

import main.stackDF
>>>>>>> 5714ae2cb167c7b6759967ec19d25dc89169b02d
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object Utils {
  def compare_type(df : DataFrame) : List[(String, Long)] = {
    var result : List[(String, Long)] = List()
<<<<<<< HEAD
    val typesDF = netflixDF.select(netflixDF("type")).distinct
=======
    val typesDF = stackDF.select(stackDF("type")).distinct
>>>>>>> 5714ae2cb167c7b6759967ec19d25dc89169b02d
    for(i <- 0 to typesDF.count.asInstanceOf[Int] - 1) {
      val typeToSearch = typesDF.collectAsList().get(i).getString(0)
      result = result :+ ((typeToSearch,  df.where("type = '" + typeToSearch + "'").count()))
    }
    result
<<<<<<< HEAD
  }
  def mostCountry(df : DataFrame): String = {
    var newDf = df.select("*").groupBy("country").count().orderBy(desc("count"))
    return  newDf.toString
  }

  def mostDirector(df : DataFrame)  : String = {
    var directorDf = df.select("*").groupBy("director").count().orderBy(desc("count"))

    return df.toString()
=======
>>>>>>> 5714ae2cb167c7b6759967ec19d25dc89169b02d
  }
}
