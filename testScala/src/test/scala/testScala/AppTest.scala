package testScala

import org.junit._
import Assert._
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import testScala.MyMain.getClass

@Test
class AppTest extends FlatSpec with Matchers {

  "iris dataset" should "contain 50 of each flower" in {

    System.setProperty("hadoop.home.dir", "C:\\Users\\gujra\\OneDrive\\Documents\\Scala\\Scala Video Tools\\Scala Video Tools\\Hadoop Tools")

    val conf = new SparkConf()
        .setAppName("Test App for Iris")
        .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val file = sc.textFile("C:\\Users\\gujra\\IdeaProjects\\testScala\\src\\main\\Resources\\iris-multiclass.csv")
    //Grouping by plant name. In the csv the plant name is the fifth column item. Mapping this to a key value pair.
    val output = file.map(line => line.split(",")).map(line => (line(4), 1)).reduceByKey((a,b) => a + b)

    //3 types of flowers
    val flowers = output.take(3)

    //Flower name and value
    //("iris setosa",  50), (), ()
    flowers(0)._2 should be (50)
    flowers(1)._2 should be (50)
    flowers(2)._2 should be (50)
  }

  "iris dataset" should "contain 150 row" in {

    System.setProperty("hadoop.home.dir", "C:\\Users\\gujra\\OneDrive\\Documents\\Scala\\Scala Video Tools\\Scala Video Tools\\Hadoop Tools")

    val conf = new SparkConf()
      .setAppName("Test App for Iris")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val file = sc.textFile("C:\\Users\\gujra\\IdeaProjects\\testScala\\src\\main\\Resources\\iris-multiclass.csv")

    //Testing class is isolation by using class primitive
    val processor = new IrisProcessor()
    processor.processIrisRdd(file) should be(150)


    //Grouping by plant name. In the csv the plant name is the fifth column item. Mapping this to a key value pair.
    val output = file.map(line => line.split(",")).map(line => (line(4), 1)).reduceByKey((a, b) => a + b)

    }

  }


