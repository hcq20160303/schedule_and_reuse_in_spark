package rddShare.main

import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-5.
 */
class SimulateRDD(
      val transformation: String,        // RDD执行的transformation
      val function: String,              // RDD执行的function
      @transient val realRDD: RDD[_],    // SimulateRDD对应的Spark RDD
      var data: String = null            // RDD的输入
     ) extends Serializable {

  var cost: Int = 0                    // RDD的估计执行代价
  @transient var parent: RDD[_] = null    // realRDD的父节点

  var inputFilename: ArrayList[String] = new ArrayList[String]          // 以该RDD为根节点的子DAG的输入
  var allTransformation: ArrayList[String] = new ArrayList[String]      // 以该RDD为根节点的子DAG中所有RDD执行的transformation操作

  def equals(other: SimulateRDD): Boolean = {
    if ((other.data == this.data) && (other.transformation == this.transformation)) {
      return true
    }
    return false
  }
}
