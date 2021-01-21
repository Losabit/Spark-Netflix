import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
  val stackDF = spark.read.option("inferSchema", "true").option("header", "true").csv("resources/netflix_titles.csv")
  println(stackDF.toString())

  // TODO :)
}
