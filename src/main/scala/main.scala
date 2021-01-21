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
  val stackDF_unamed = spark.read.option("inferSchema", "true").option("header", "true").csv("resources/netflix_titles.csv")
  val dfColumnsName = Seq("show_id", "type", "title", "director", "cast", "country", "date_added",
    "release_year", "rating", "duration", "listed_in", "description")
  val stackDF = stackDF_unamed.toDF(dfColumnsName.seq:_*)
  println(stackDF.printSchema())
  // TODO :)
}
