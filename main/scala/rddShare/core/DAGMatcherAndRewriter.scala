package rddShare.core

import java.util
import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-9.
 *
 * 匹配及改写：该object将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
 */

object DAGMatcherAndRewriter {

  def dagMatcherAndRewriter(finalRDD: RDD[_], nodesList: ArrayList[SimulateRDD], indexOfDagScan: ArrayList[Integer]): Unit = {
    transformDAGtoList(null, finalRDD, nodesList, indexOfDagScan)
    val repository = CacheManager.getRepository
    if ( repository.size() != 0 ){
      /**
       * 将输入dag和仓库一一进行匹配
       */
      val ite = repository.iterator()
      while ( ite.hasNext ){
        val cacheMetaData = ite.next()
        val indexOfCacheDagScan = new util.ArrayList[Integer]
        val cacheNodesList = new util.ArrayList[SimulateRDD]
        transformDAGtoList(null, cacheMetaData.root.realRDD, cacheNodesList, indexOfCacheDagScan)
        if ( nodesList.size() >= cacheNodesList.size() && indexOfDagScan.size() >= indexOfCacheDagScan.size()) {
          /**
           * 将cache和DAG中的每个Load操作符进行比较
           */
          val ite = indexOfDagScan.iterator()
          while ( ite.hasNext ) {
            val idOfDagScan = ite.next()
            indexOfDagScan.remove(idOfDagScan)
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
            while (index < cacheNodesList.size() && isMatch) {
              if (cacheNodesList.get(index).equals(nodesList.get(indexOfdag))) {
                index = index+1
                indexOfdag = indexOfdag+1
              } else {
                isMatch = false
              }
            }
            /**
             * Rewriter
             */
            if (isMatch) {   // 完全匹配则改写DAG
              // check if this file is exist
              val exist = CacheManager.fileExist(cacheMetaData.outputFilename)
              if ( exist ){
                val realRDD = nodesList.get(indexOfdag - 1).realRDD
                val rewriter = finalRDD.sparkContext.objectFile(cacheMetaData.outputFilename)
                val parent = nodesList.get(indexOfdag - 1).realRDDparent
                parent.changeDependeces(rewriter)
              }else{
                CacheManager.synchronized(CacheManager.removeCache(cacheMetaData.outputFilename))
              }
            }
          }
        }
      }
    }
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
      node.dependencies.map(_.rdd).foreach(child => transformDAGtoList(node, child, nodesList, indexOfDagScan))
    }

    val simulateRDD = new SimulateRDD(node.transformation, node.function, node)
    /**
     * 判断RDD的操作是否是表扫描或者读取外部数据
     */
    nodesList.add(simulateRDD)
    if ( node.transformation.equalsIgnoreCase("hadoopFile") ){
      val index = nodesList.indexOf(simulateRDD)
      node.indexOfnodesList = index    // 记录下该RDD在nodesList的位置，以后需要通过该下标找到RDD对应的SimulateRDD
      node.indexOfleftInNodesList = index
      indexOfDagScan.add(index)
      simulateRDD.inputFileName.add(node.name)
    }
    /**
     * bug2:根节点的allTransformation没有赋值
     */
    simulateRDD.allTransformation.add(simulateRDD.transformation)
    // 为父节点增加数据
    if ( parent != null ){
      if ( parent.indexOfleftInNodesList == -1 ){
        parent.indexOfleftInNodesList = node.indexOfleftInNodesList
      }
      nodesList.get(parent.indexOfnodesList).inputFileName.addAll(simulateRDD.inputFileName)
      /**
       * bug3:parent.allTransformation没有将子节点的allTransformation加入
       */
      nodesList.get(parent.indexOfnodesList).allTransformation.addAll(simulateRDD.allTransformation)
    }
  }
}
