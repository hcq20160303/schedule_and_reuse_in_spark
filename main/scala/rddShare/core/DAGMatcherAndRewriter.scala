package rddShare.core

import java.util.ArrayList
import java.util.function.Consumer

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-9.
 *
 * 匹配及改写：该object将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
 */

object DAGMatcherAndRewriter {

  def dagMatcherAndRewriter(rddShare: RDDShare, finalRDD: RDD[_], nodesList: ArrayList[SimulateRDD], indexOfDagScan: ArrayList[Integer]): Unit = {
    transformDAGtoList(null, finalRDD, nodesList, indexOfDagScan)
    val repository = CacheManager.getRepository
    println("DAGMatcherAndRewriter.dagMatcherAndRewriter: CacheManager.getRepository")
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
          "sizoOfOutputData: " + t.sizeOfOutputData + "\tuse: " + t.use)
      }
    })
    if ( repository.size() != 0 ){
      /**
       * 将输入dag和仓库一一进行匹配
       */
      val ite = repository.iterator()
      while ( ite.hasNext ){
        val cacheMetaData = ite.next()
        val indexOfCacheDagScan = cacheMetaData.indexOfDagScan
        val cacheNodesList = cacheMetaData.nodesList
        if ( nodesList.size() >= cacheNodesList.length && indexOfDagScan.size() >= indexOfCacheDagScan.size()) {
          /**
           * 将cache和DAG中的每个Load操作符进行比较
           */
          val ite = indexOfDagScan.iterator()
          val hasCheck = new ArrayList[Integer]()
          while ( ite.hasNext ) {
            val idOfDagScan = ite.next()
            hasCheck.add(idOfDagScan)
//            indexOfDagScan.remove(idOfDagScan)
            /**
             * Matcher
             */
            var index: Int = 0  // cache中Scan操作的位置
            /**
             * bug9: 对于dag中有多个输入的情况，当有一个输入匹配成功，那么后一个输入在dag中的位置则会改变
             * 因此，变量indexOfDagScan不是固定的，需要根据匹配情况更改.
             */
            var indexOfdag: Int = idOfDagScan  // dag中Scan操作的位置（可能有多个Scan操作）
            var isMatch = true
            while (index < cacheNodesList.length && isMatch) {
              if (cacheNodesList(index).equals(nodesList.get(indexOfdag))) {
                index = index+1
                indexOfdag = indexOfdag+1
              } else {
                isMatch = false
              }
            }
            /**
             * Rewriter
             */
            println("DAGMatcherAndRewriter.scala---isMatch: " + isMatch)
            if (isMatch) {   // 完全匹配则改写DAG
              // check if the input files and the output file is exist
              // 1. check input files
              var inputFileExist = true
              cacheMetaData.root.inputFileName.forEach(new Consumer[String] {
                override def accept(t: String): Unit = {
                  if ( !CacheManager.fileExist(t, "input")){
                    inputFileExist = false
                  }
                }
              })
              // 2. check output files
              if ( inputFileExist && CacheManager.fileExist(cacheMetaData.outputFilename, "output") ){
                // after check exist, then check if these files have modified by others
                println("DAGMatcherAndRewriter.scala---file exist: " + true)
                if ( CacheManager.checkFilesNotModified(cacheMetaData)){
                  println("DAGMatcherAndRewriter.scala---file modified: " + false)
                  println("DAGMatcherAndRewriter.scala---outputFilename: " + cacheMetaData.outputFilename)
                  val realRDD = nodesList.get(indexOfdag - 1).realRDD
                  val rewriter = finalRDD.sparkContext.objectFile(cacheMetaData.outputFilename, realRDD.partitions.size)
                  val parent = nodesList.get(indexOfdag - 1).realRDDparent
                  if ( parent == null){
                    rddShare.setFinalRDD(rewriter)
                  } else {
                    parent.changeDependeces(rewriter)
                  }
                  cacheMetaData.use += 1   // use add 1 to judge if it should be replaced for future
                }else{
                  println("DAGMatcherAndRewriter.scala---file modified: " + true)
                }
              }else{
                println("DAGMatcherAndRewriter.scala---file exist: " + false)
              }
            }
          }
          indexOfDagScan.removeAll(hasCheck)
          hasCheck.clear()
        }
      }
    }
    // the dependencies of finalRdd may be changed, so we need transform it again
    nodesList.clear()
    indexOfDagScan.clear()
    transformDAGtoList(null, rddShare.getFinalRDD, nodesList, indexOfDagScan)
  }

  /**
   * 将指定的DAG图按深度遍历的顺序得到DAG图中的各个节点
   */
  private def transformDAGtoList( parent: RDD[_], node: RDD[_], nodesList: ArrayList[SimulateRDD], indexOfDagScan: ArrayList[Integer] ): Unit = {
    /**
     * your code in here
     */
    if ( node == null ){
      return
    }

    if ( node.dependencies != null ) {
      println("rdd" + node.id + "'s dependencies: " + node.dependencies.toString())
      node.dependencies.map(_.rdd).foreach(child => transformDAGtoList(node, child, nodesList, indexOfDagScan))
    }

    val simulateRDD = new SimulateRDD(node.transformation, node.function)
    simulateRDD.realRDD = node
    /**
     * 判断RDD的操作是否是表扫描或者读取外部数据
     */
    nodesList.add(simulateRDD)
    val index = nodesList.indexOf(simulateRDD)
    node.indexOfnodesList = index    // 记录下该RDD在nodesList的位置，以后需要通过该下标找到RDD对应的SimulateRDD
    if ( node.transformation.equalsIgnoreCase("textFile") || node.transformation.equalsIgnoreCase("objectFile")  ){
      node.indexOfleafInNodesList = index
      indexOfDagScan.add(index)
      simulateRDD.inputFileName.add(node.name)
      val modifiedTime = CacheManager.getLastModifiedTimeOfFile(node.name)
      simulateRDD.inputFileLastModifiedTime.add(modifiedTime)
    }
    /**
     * bug2:根节点的allTransformation没有赋值
     */
    simulateRDD.allTransformation.add(simulateRDD.transformation)
    // pull data from childs
    node.dependencies.map(_.rdd).foreach(child => {
      if (node.indexOfleafInNodesList == -1 ){
        node.indexOfleafInNodesList = child.indexOfleafInNodesList
      }
      simulateRDD.inputFileName.addAll(nodesList.get(child.indexOfnodesList).inputFileName)
      simulateRDD.inputFileLastModifiedTime.addAll(nodesList.get(child.indexOfnodesList).inputFileLastModifiedTime)
      simulateRDD.allTransformation.addAll(nodesList.get(child.indexOfnodesList).allTransformation)
    })
  }
}
