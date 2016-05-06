package rddShare.main

import java.util._

import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.parsing.json.JSON


/**
 * Created by hcq on 16-5-5.
 */
class RDDShare(private val finalRDD: RDD[_]) {

  private val nodesList = new ArrayList[SimulateRDD]    // 按深度遍历的顺序得到DAG图的各个节点
  private val cacheRDD = new ArrayList[RDD[_]]             // DAG中需要缓存的RDD
  private val indexOfDagScan = new ArrayList[Integer]      // DAG的输入

  /**
   * 将指定的DAG图按深度遍历的顺序得到DAG图中的各个节点
   */
  private def transformDAGtoList( parent: RDD[_], node: RDD[_] ): Unit = {
    /**
     * your code in here
     */
    if ( node == null ){
      return
    }

    if ( node.dependencies != null ) {
      node.dependencies.map(_.rdd).foreach(child => transformDAGtoList(node, child))
    }

    val simulateRDD = new SimulateRDD(node.transformation, node)
    /**
     * 判断RDD的操作是否是表扫描或者读取外部数据
     */
    nodesList.add(simulateRDD)
    if ( node.name.contains("""/""") ){
      val index = nodesList.indexOf(simulateRDD)
      node.indexOfnodesList = index    // 记录下该RDD在nodesList的位置，以后需要通过该下标找到RDD对应的SimulateRDD
      indexOfDagScan.add(index)
      simulateRDD.inputFilename.add(node.transformation)
    }
    /**
     * bug2:根节点的allTransformation没有赋值
     */
    simulateRDD.allTransformation.add(simulateRDD.transformation)
    // 为父节点增加数据
    if ( parent != null ){
      nodesList.get(parent.indexOfnodesList).inputFilename.addAll(simulateRDD.inputFilename)
      /**
       * bug3:parent.allTransformation没有将子节点的allTransformation加入
       */
      nodesList.get(parent.indexOfnodesList).allTransformation.addAll(simulateRDD.allTransformation)
    }
  }

  /**
   * 匹配及改写函数：该函数将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
   */
  def dagMatcherAndRewriter(): Unit = {
    transformDAGtoList(null, finalRDD)
  }

  /**
   * 缓存挑选函数：该函数从输入的DAG当中选择需要缓存的子DAG
   */
  def getCacheRDD(): Unit = {

  }

  /**
   * 缓存管理函数：该函数完成缓存的管理工作，当出现以下情况之一触发该操作：
   * 1） 缓存总大小超过设定阈值；
   * 2） 缓存超过设定时间未更新；
   * 3） 缓存中的某个DAG的输入被删除或者被修改。
   */
  def cacheManage(): Unit = {
  }

}

object RDDShare{

  def main(args: Array[String]): Unit ={
  }

  private var repository: TreeSet[CacheMetaData] = new TreeSet[CacheMetaData](new Comparator[CacheMetaData]() {
    /**
     * 排序规则：
     * 1. dag树的节点数量越多越靠前
     * 2. “加载数据”操作符（Scan）越多，则越靠前
     * 3. 操作符优先级
     * 为什么需要排序？是为了保证第一次匹配成功的dag就是最大匹配
     */
    def compare(o1: CacheMetaData, o2: CacheMetaData): Int = {
      if (o1.nodesList.size() > o2.nodesList.size() ) {       // 1. dag树的节点数量越多越靠前
        return -1
      }
      else if (o1.nodesList.size() < o2.nodesList.size() ) {
        return 1
      }
      else {
        val o1inputFilenames: ArrayList[String] = o1.root.inputFilename
        val o2inputFilenames: ArrayList[String] = o2.root.inputFilename
        if (o1inputFilenames.size > o2inputFilenames.size) {   // 2. “加载数据”操作符（Scan）越多，则越靠前
          return -1
        }
        else if (o1inputFilenames.size < o2inputFilenames.size) {
          return 1
        }
        else {
          var compare: Int = 0
          for( i <- 0 to o1inputFilenames.size-1){
            compare = o1inputFilenames.get(i).compareToIgnoreCase(o2inputFilenames.get(i))
            if ( compare != 0 ){
              return -compare
            }
          }
          val o1allTransformation: ArrayList[String] = o1.root.allTransformation
          val o2allTransformation: ArrayList[String] = o2.root.allTransformation

          for ( i <- 0 to o1allTransformation.size-1) {
            compare = TRANSFORMATION_PRIORITY.get(o1allTransformation.get(i)) - TRANSFORMATION_PRIORITY.get(o2allTransformation.get(i))
            if (compare != 0) {
              return -compare
            }
          }
          return 0
        }
      }
    }
  })

  private val CACHE_TRANSFORMATION: Set[String] = TRANSFORMATION_PRIORITY.keySet()

  private val TRANSFORMATION_PRIORITY: HashMap[String, Integer] = initTRANSFORMATION_PRIORITY()
  private def initTRANSFORMATION_PRIORITY(): HashMap[String, Integer] = {
    val tranformtion_priority = new HashMap[String, Integer]
    val jsonLines = Source.fromFile("../../../resources/rddShare/transformation.json").getLines()
    jsonLines.foreach( line => {
      val trasformationAndPriority = JSON.parseFull(line)
      trasformationAndPriority match {
        case Some(m: Map[String, Any]) => {
          tranformtion_priority.put(m.get("transformation").toString,
            m.get("priority") match{
            case pri: Int => pri
          } )
        }
      }}
    )
    tranformtion_priority
  }

}


