import java.util.Dictionary

import main.stackDF
import org.apache.spark.sql.DataFrame

object Utils {
  def compare_type(df : DataFrame) : List[(String, Long)] = {
    var result : List[(String, Long)] = List()
    val typesDF = stackDF.select(stackDF("type")).distinct
    for(i <- 0 to typesDF.count.asInstanceOf[Int] - 1) {
      val typeToSearch = typesDF.collectAsList().get(i).getString(0)
      result = result :+ ((typeToSearch,  df.where("type = '" + typeToSearch + "'").count()))
    }
    result
  }
}
