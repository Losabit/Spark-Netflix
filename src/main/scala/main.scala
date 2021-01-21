import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import Utils._
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
  println(netflixDF.printSchema())
  Utils.fieldToList(netflixDF, 4).show(20)
/*
  println(NetflixData.averageShowDuration(NetflixData.showTypes(1),netflixDF))
  println(mostDirector(netflixDF))
  println(NetflixData.typeIteration(netflixDF))
*/
}
