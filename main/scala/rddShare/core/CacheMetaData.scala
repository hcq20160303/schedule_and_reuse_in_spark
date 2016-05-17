package rddShare.core

import java.sql.Timestamp
import java.util

/**
 * Created by hcq on 16-5-5.
 * This class use to save the meta data of a cache
 */
class CacheMetaData(
      val id: Int,                                  // the auto-inc key in mysql table
      val nodesList: Array[SimulateRDD],            // DAG图的各个节点
      val indexOfDagScan: util.ArrayList[Integer],  // the leaf nodes(read file) of this DAG
      val outputFilename: String,                   // 结果保存的文件名
      val outputFileLastModifiedTime: Long,       // use to maintain consistency
      val sizeOfOutputData: Double,
      val exeTimeOfDag: Long,
      val insertTime: Timestamp
     ) extends Serializable {

  val root = nodesList(nodesList.length-1)  // DAG的根节点
  var reuse: Int = 0

  override def toString: String ={
    "nodesList: " + nodesList.map(_.toString()) + "\tindexOfDagScan: "+indexOfDagScan.toString +
    "outputFilename: " + outputFilename + "\toutputFileLastModifiedTime: " + outputFileLastModifiedTime +
    "sizoOfOutputData: " + sizeOfOutputData + "\texeTimeOfDag: " + exeTimeOfDag
  }
}
