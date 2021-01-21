import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{lit, mean, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import NetflixData._

object main extends App {
  // Defining the spark Session
  val spark = SparkSession
    .builder()
    .appName("SparkNetflix")
    .master("local[*]")
    .getOrCreate()

  // Setting log level to ERROR to Avoid multiple INFO logs
  spark.sparkContext.setLogLevel("ERROR")

  // Reading the DataFrame from source File
  val netflixDF = spark.read.option("inferSchema", "true").option("header", "true").csv("resources/netflix_titles.csv")
  println(netflixDF.toString())

  var netflixData = new NetflixData()
  // TODO :)


  // MOYENNE Duree Film / Serie
  println(netflixData.averageShowDuration(netflixData.showTypes(1),netflixDF))

}
