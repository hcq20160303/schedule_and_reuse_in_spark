package rddShare.main

import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-5.
 */
class SimulateRDD {

  def this(data: String, transformation: String, realRDD: RDD, whichCache: CacheMetaData = null) {
    this()
    inputFilename = new ArrayList[String]
    allTransformation = new ArrayList[String]
    this.data = data
    this.transformation = transformation
    this.realRDD = realRDD
    this.fromCache = true
    this.whichCache = whichCache
  }

  var data: String = null              // RDD的输入
  var transformation: String = null    // RDD经过的变换操作
  var cost: Int = 0                    // RDD的估计执行代价
  var depedences: ArrayList[SimulateRDD] = new ArrayList[SimulateRDD]   //RDD的依赖
  @transient var realRDD: RDD = null   // the RDD in Spark that form the SimulateRDD

  var candidateCache: Boolean = false                                   // 是否是需要缓存而生成的新DAG的finalRDD
  var inputFilename: ArrayList[String] = new ArrayList[String]          // 以该RDD为根节点的子DAG的输入
  var allTransformation: ArrayList[String] = new ArrayList[String]      // 以该RDD为根节点的子DAG中所有RDD执行的transformation操作

  var fromCache: Boolean = false         // 该RDD的输入是否来自于Repository中的缓存
  var whichCache: CacheMetaData = null   // 如果输入来自于Repository，记录下该缓存

  def equals(other: SimulateRDD): Boolean = {
    if ((other.data == this.data) && (other.transformation == this.transformation)) {
      return true
    }
    return false
  }

}

object SimulateRDD {

}
