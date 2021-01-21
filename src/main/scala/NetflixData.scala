import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, regexp_replace}

object NetflixData {

  val showTypes = List("TV Show","Movie")

  def averageShowDuration(showType: String,df: DataFrame): Double = {
    var showDF = df.filter(df("type") === showType)
    if(showType == showTypes.head){
      // SERIES
      showDF = showDF.withColumn("duration2", regexp_replace(showDF("duration")," Seasons",""))
    }else {
      // FILM
      showDF = showDF.withColumn("duration2", regexp_replace(showDF("duration")," min", ""))
    }
    showDF.select(mean(showDF("duration2"))).first().getDouble(0)
  }
}
