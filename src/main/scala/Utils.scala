import java.util.Dictionary
import main.netflixDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

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
  def mostCountry(df : DataFrame): String = {
    var newDf = df.select("*").groupBy("country").count().orderBy(desc("count"))
    return  newDf.toString
  }

  def mostDirector(df : DataFrame)  : String = {
    var directorDf = df.select("*").groupBy("director").count().orderBy(desc("count"))

    return directorDf.toString

  }
}
