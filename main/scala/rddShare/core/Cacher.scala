package rddShare.core

import java.io.File
import java.util
import java.util.ArrayList
import java.util.function.Consumer

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Created by hcq on 16-5-9.
 */
object Cacher {

  private val sparkCorePath = getClass.getResource("").getPath.split("target")(0)
  private val resourcesPath = sparkCorePath + "src/main/resources/rddShare/"
  private val conf = ConfigFactory.parseFile(new File(resourcesPath + "default.conf"))

  // a RDD which execute a transformation in CACHE_TRANSFORMATION will be chosen to
  // store in repostory, and reuse by other application
  private val CACHE_TRANSFORMATION: Predef.Set[String] =
    conf.getString("rddShare.cacheTransformation").split(" ").toSet

  def getCacheRDD(nodesList: ArrayList[SimulateRDD]): Unit = {
    val size = nodesList.size()
    for ( i <- (size-1) to 0){
      val node = nodesList.get(i)
      // cache this RDD if this RDD is contained by the CACHE_TRANSFORMATION
      if ( CACHE_TRANSFORMATION.contains(node.transformation)){
        node.realRDD.isCache = true

        val cachePath = CacheManager.getRepositoryBasePath + node.realRDD.sparkContext.hashCode()+ "/" +
          node.realRDD.transformation + "["+node.realRDD.id+"]"
        node.realRDD.cache()

        val begin = System.currentTimeMillis()
        node.realRDD.saveAsObjectFile(cachePath)
        val end = System.currentTimeMillis()

        var fileSize = .0
        if ( CacheManager.getRepositoryBasePath.contains("hdfs")){   // use hdfs to cache the data
          val config = new Configuration()
          val path = new Path(cachePath)
          val hdfs = path.getFileSystem(config)
          val cSummary = hdfs.getContentSummary(path)
          fileSize = cSummary.getLength().toDouble/math.pow(1024, 3)
        }else{                                                       // use the local file to cache the data
          fileSize = (new File(cachePath)).length().toDouble/math.pow(1024, 3)
        }

        val modifiedTime = CacheManager.getLastModifiedTimeOfFile(cachePath)

        val cacheNodes = nodesList.subList(node.realRDD.indexOfleftInNodesList, node.realRDD.indexOfnodesList)
        val indexOfDagScan = new util.ArrayList[Integer]
        cacheNodes.forEach{ new Consumer[SimulateRDD] {
          override def accept(t: SimulateRDD): Unit = {
            if ( t.transformation.equalsIgnoreCase("hadoopFile")){
              indexOfDagScan.add(cacheNodes.indexOf(t))
            }
          }
        }}
        val addCache = new CacheMetaData(cacheNodes, indexOfDagScan
                                         , cachePath, modifiedTime, fileSize, (end-begin))
        CacheManager.checkCapacityEnoughElseReplace(addCache)
      }
    }
  }
}
