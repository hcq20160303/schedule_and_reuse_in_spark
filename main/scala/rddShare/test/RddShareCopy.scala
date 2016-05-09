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
    val slices = 2
    val n = math.min(10L * slices, Int.MaxValue).toInt // avoid overflow
    val rdd1 = sc.parallelize(1 until n, slices).map(x => (x, 1))
    rdd1.saveAsObjectFile("/home/hcq/Documents/spark_1.5.0/cache/rdd1")
//    val rdd3 = sc.objectFile[rdd1]("/home/hcq/Documents/spark_1.5.0/cache")
    val rdd2 = sc.objectFile[rdd1.type]("/home/hcq/Documents/spark_1.5.0/cache/rdd1")
    val rdd3 = rdd1.map(x => (x._1, 3))
    rdd3.changeDependeces(rdd2)
    rdd3.foreach(print )
  }

  def stopSpark() {
    sc.stop()
  }
}
