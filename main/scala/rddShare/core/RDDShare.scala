package rddShare.core

//import java.util._

import java.io._
import java.util.ArrayList

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD


/**
 * Created by hcq on 16-5-5.
 * RDDShare System contain three components:
 * 1. DAGMatcherAndRewriter: use to match a dag with repository and rewrite it use the data matched
 * 2. Cacher: use to select some sub-dag to cache to the repository
 * 3. CacheManager: use to manager the repository
 */
class RDDShare(private val finalRDD: RDD[_]) {

  private val nodesList = new ArrayList[SimulateRDD]       // 按深度遍历的顺序得到DAG图的各个节点
  private val cacheRDD = new ArrayList[RDD[_]]             // DAG中需要缓存的RDD
  private val indexOfDagScan = new ArrayList[Integer]      // DAG的输入

  /**
   * 匹配及改写：该object将一个输入的DAG和缓存当中的所有DAG进行匹配找到可重用的缓存并改写当前的DAG
   */
  def dagMatcherAndRewriter: Unit ={
    DAGMatcherAndRewriter.dagMatcherAndRewriter(finalRDD, nodesList, indexOfDagScan)
  }

  /**
   * 缓存挑选函数：该函数从输入的DAG当中选择需要缓存的子DAG
   */
  def getCache: Unit ={
    Cacher.getCacheRDD(nodesList)
  }
}

object RDDShare{

  val sparkCorePath = getClass.getResource("").getPath.split("target")(0)
  val resourcesPath = sparkCorePath + "src/main/resources/rddShare/"
  val conf = ConfigFactory.parseFile(new File(resourcesPath + "default.conf"))

  def getAnnoFunctionCopyPath = conf.getString("rddShare.annoFunctionCopyPath")

  /**
   * 缓存管理函数：该函数完成缓存的管理工作，当出现以下情况之一触发该操作：
   * 1. replace
   * * 1） 缓存总大小超过设定阈值；
   * * 2） 缓存超过设定时间未更新；
   * 2. maintain consistency
   * * 1) 缓存中的某个DAG的输入被删除或者被修改。
   */
  def cacheManager(manageType: String, needCacheSize: Double, deleteData: String = null): Unit ={
    CacheManager.synchronized(CacheManager.replaceCache(manageType, needCacheSize, deleteData))
  }
}