import Utils._
import org.apache.spark.sql.{Dataset, SparkSession}

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
  val dataset :  Dataset[(String,String)] = Utils.fieldToList(netflixDF, 5)
  //.withColumnRenamed("_1", "id").withColumnRenamed("_2", "director")

  // Le plus gros pays producteur de serie ou film netflix
  println("Le pays le plus représenté" )
  println(mostElem(dataset, "country"))
  println("---------------------------------------")
  // Le type le mieux noté
  println("Le type  le mieux noté  : ")
  println(getMostRatedType(netflixDF))

  println("---------------------------------------")
  val datasetD :  Dataset[(String,String)] = Utils.fieldToList(netflixDF, 3)
  println("Directeur le plus présent sur Netflix : ")
  // Le directeur le plus représenté
  println(mostElem(datasetD, "director"))

  println("---------------------------------------")

  //  Temps moyens
  println("Temps moyen d'un show   : ")
  println(NetflixData.averageShowDuration(NetflixData.showTypes(1),netflixDF))
  println("---------------------------------------")

  println(NetflixData.typeIteration(netflixDF))
  println("---------------------------------------")
  println("Older film : ")
  val datasetOlder :  Dataset[(String,String)] = Utils.fieldToList(netflixDF, 7)
  println(getOlder(datasetOlder,"release_year"))
  println("Le plus recent  film : ")
  println(getNew(datasetOlder,"release_year"))


}
