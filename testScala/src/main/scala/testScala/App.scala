package testScala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object MyMain {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\gujra\\OneDrive\\Documents\\Scala\\Scala Video Tools\\Scala Video Tools\\Hadoop Tools")

    /*val conf = new SparkConf()
                    .setAppName("Test Scala Application")
                      //Creating a new app locally with 2 threads
                      .setMaster("local[2]")

    //Creating a new Spark context
    val sc = new SparkContext(conf)*/

    val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Test Project")
    .config(new SparkConf())
      .getOrCreate()

    val filepath = getClass.getResource("/iris-multiclass.csv").getPath
    //Fetching text file with the context
    val myFile = spark.read.
      format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").
      load(filepath)
    println(myFile.count())

    //Stop
    spark.stop()
  }

}
