package testScala

import org.apache.spark.rdd.RDD

/**
  * Created by gujra on 23/05/2017.
  */
class IrisProcessor {

    def processIrisRdd(rdd: RDD[String]) : Int ={

      rdd.collect.length
    }

}
