package rddShare.core

import java.util

/**
 * Created by hcq on 16-5-5.
 * This class use to save the meta data of a cache
 */
class CacheMetaData(
      val nodesList: util.List[SimulateRDD],        // DAG图的各个节点
      val indexOfDagScan: util.ArrayList[Integer],  // the leaf nodes(read file) of this DAG
      val outputFilename: String,                   // 结果保存的文件名
      val outputFileLastModifiedTime: Long,         // use to maintain consistency
      val sizoOfOutputData: Double,
      val exeTimeOfDag: Long
     ) extends Serializable {

  @transient val root = nodesList.get(nodesList.size()-1)  // DAG的根节点
  var use: Int = 0
}
