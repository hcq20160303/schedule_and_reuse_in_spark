package rddShare.test

import org.apache.spark.{SparkContext, SparkConf}
import rddShare.core.CacheManager

/**
 * Created by hcq on 16-5-10.
 */
object testWordCount {

  private val conf = new SparkConf().setAppName("RDDShare")
    .setMaster("local")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/hcq/Documents/spark_1.5.0/eventLog")
  private val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    CacheManager.initRepository

    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/part-00151").flatMap(line => line.split(" ")).
      map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.collect()

    stopSpark()
  }

  def stopSpark() {
    CacheManager.saveRepository
    sc.stop()
  }

}
