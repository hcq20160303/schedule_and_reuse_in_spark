package rddShare.core

import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-5.
 * This class use to simulate the RDD in Spark, but it's very simple,
 * as it just contains transformation, function, and inputFileName of a RDD,
 * it use to match by DAGMatcherAndRewriter Component in RDDShare System.
 */
class SimulateRDD(
      val transformation: String,        // RDD执行的transformation
      val function: String,              // RDD执行的function
      @transient val realRDD: RDD[_]     // SimulateRDD对应的Spark RDD
     ) extends Serializable {

  @transient var cost: Int = 0           // RDD的估计执行代价
  @transient var realRDDparent: RDD[_] = null   // realRDD的父节点

  var inputFileName: ArrayList[String] = new ArrayList[String]          // 以该RDD为根节点的子DAG的输入
  var inputFileLastModifiedTime: ArrayList[Long] = new ArrayList[Long]  // the last modified time of these file, this use to consistency maintain by CacheManager
  var allTransformation: ArrayList[String] = new ArrayList[String]      // 以该RDD为根节点的子DAG中所有RDD执行的transformation操作

  def equals(other: SimulateRDD): Boolean = {
    if (other.inputFileName.equals(this.inputFileName)
      && other.transformation.equalsIgnoreCase(this.transformation)
      && other.function.equalsIgnoreCase(this.function)) {
      return true
    }
    return false
  }
}
