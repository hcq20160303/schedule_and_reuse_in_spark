package rddShare.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcq on 16-4-19.
 */

object RddShareCopy {

  private val conf = new SparkConf().setAppName("RDDShare")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/hcq/Documents/spark_1.5.0/eventLog")
  private val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val re = sc.objectFile("/home/hcq/Documents/spark_1.5.0/repository/8969451351462891627711reduceByKey")
    re.foreach(println)
  }

  def stopSpark() {
    sc.stop()
  }
}
