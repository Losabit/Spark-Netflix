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

  val dataset :  Dataset[(String,String)] = Utils.fieldToList(netflixDF, 5)
  //.withColumnRenamed("_1", "id").withColumnRenamed("_2", "director")
  println(mostElem(dataset,"country"))
/*
  println(NetflixData.averageShowDuration(NetflixData.showTypes(1),netflixDF))
  println(mostDirector(netflixDF))
  println(NetflixData.typeIteration(netflixDF))
*/
}
