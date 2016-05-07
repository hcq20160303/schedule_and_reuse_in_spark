package rddShare.main

import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-5.
 */
class SimulateRDD(
      val transformation: String,        // RDD执行的transformation
      @transient val realRDD: RDD[_],    // SimulateRDD对应的Spark RDD
      @transient var whichCache: CacheMetaData = null,  // 如果输入来自于Repository，记录下该缓存
      var data: String = null            // RDD的输入
     ) extends Serializable {

  var cost: Int = 0                    // RDD的估计执行代价
  @transient var parent: RDD[_] = null    // realRDD的父节点

  @transient var candidateCache: Boolean = false                        // 是否是需要缓存而生成的新DAG的finalRDD
  var inputFilename: ArrayList[String] = new ArrayList[String]          // 以该RDD为根节点的子DAG的输入
  var allTransformation: ArrayList[String] = new ArrayList[String]      // 以该RDD为根节点的子DAG中所有RDD执行的transformation操作

  @transient var fromCache: Boolean = false    // 该RDD的输入是否来自于Repository中的缓存
  def setWhichCache( whichCache: CacheMetaData ): Unit = {
    this.whichCache = whichCache
    fromCache = true
  }

  def equals(other: SimulateRDD): Boolean = {
    if ((other.data == this.data) && (other.transformation == this.transformation)) {
      return true
    }
    return false
  }
}
