package rddShare.test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hcq on 16-5-10.
 */
object testWordCount {

  private val conf = new SparkConf().setAppName("WordCount")
    .setMaster("local")
//    .setMaster("spark://192.168.1.105:7077")
    .set("spark.executor.memory", "2g")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/hcq/Documents/spark_1.5.0/eventLog")
  private val sc = new SparkContext(conf)

  def main(args: Array[String]) {
//    CacheManager.initRepository
  // big input size
    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/Rankings/").flatMap(line => line.split(" ")).
      map(word => (word, 1)).reduceByKey(_ + _)
    // small input size
//    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/part-00151").flatMap(line => line.split(" ")).
//        map(word => (word, 1)).reduceByKey(_ + _)
    // test replace cache
//    val wordCount = sc.textFile("/home/hcq/Documents/spark_1.5.0/input/Rankings/part-00003").flatMap(line => line.split(" ")).
//        map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.saveAsTextFile("/home/hcq/Documents/spark_1.5.0/output/"+ System.currentTimeMillis()+wordCount.transformation)

    stopSpark()
  }

  def stopSpark() {
//    CacheManager.saveRepository
    sc.stop()
  }

}
