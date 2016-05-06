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
    val rdd1 = sc.parallelize(1 until n, slices).map(x => (x, 1)).collect()
  }

  def stopSpark() {
    sc.stop()
  }
}
