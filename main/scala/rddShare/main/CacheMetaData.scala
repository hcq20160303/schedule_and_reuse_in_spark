package rddShare.main

import java.util.ArrayList

import scala.reflect.ClassTag

/**
 * Created by hcq on 16-5-5.
 */
class CacheMetaData(
      val nodesList: ArrayList[SimulateRDD],    // DAG图的各个节点
      val outputFilename: String                // 结果保存的文件名
     ) extends Serializable {

  @transient val root = nodesList.get(nodesList.size()-1)  // DAG的根节点
  var sizoOfInputData: Double = .0
  var sizoOfOutputData: Double = .0
  var exeTimeOfDag: Double = .0

}
