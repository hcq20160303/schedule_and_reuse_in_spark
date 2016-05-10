package rddShare.core

import java.io._
import java.nio.file.{Paths, Files}
import java.util.function.Consumer
import java.util.{ArrayList, Comparator, TreeSet, HashMap}

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Created by hcq on 16-5-9.
 */
object CacheManager {

  val sparkCorePath = getClass.getResource("").getPath.split("target")(0)
  val resourcesPath = sparkCorePath + "src/main/resources/rddShare/"
  val conf = ConfigFactory.parseFile(new File(resourcesPath + "default.conf"))
  private val repositoryBasePath: String = conf.getString("rddShare.repositoryBasePath")
  def getRepositoryBasePath = repositoryBasePath

  // the max space size in repository, if beyond this size,
  // it will trigger the cacheManager method
  private val repositoryCapacity: Double = conf.getString("rddShare.repositoryCapacity").split("g")(0).toDouble
  def getRepositoryCapacity = repositoryCapacity
  var repositorySize: Double = 0  // the size of repository at now

  // this used by repository to sort the cacheMetaDatas
  private val TRANSFORMATION_PRIORITY: HashMap[String, Integer] = {

    val tranformtion_priority = new HashMap[String, Integer]

    val jsonLines = Source.fromFile(resourcesPath + "transformation.json").getLines()
    jsonLines.foreach( line => {
      val transformationAndPriority = JSON.parseFull(line)
      transformationAndPriority match {
        case Some(m: Map[String, Any]) => {
          tranformtion_priority.put(
            m.get("transformation") match { case Some(tran: Any) => tran.toString },
            m.get("priority")       match { case Some(pri: Any) => pri.asInstanceOf[Double].toInt }
          )
        }
      }}
    )

    tranformtion_priority
  }
  def tranformtion_priority = TRANSFORMATION_PRIORITY

  private val repository: TreeSet[CacheMetaData] = new TreeSet[CacheMetaData](new Comparator[CacheMetaData]() {
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
        val o1inputFilenames: ArrayList[String] = o1.root.inputFileName
        val o2inputFilenames: ArrayList[String] = o2.root.inputFileName
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
  def initRepository: Unit = {
    // load the history cacheMetaData to repository from disk if rddShare system has the history data
    if (Files.exists(Paths.get(resourcesPath+"repository"))){
      val input = new ObjectInputStream(new FileInputStream(resourcesPath + "repository"))
      val repo = input.readObject().asInstanceOf[TreeSet[CacheMetaData]]
      input.close()
      val ite = repo.iterator()
      while (ite.hasNext){
        val cache = ite.next()
        repository.add(cache)
        repositorySize += cache.sizoOfOutputData
      }
    }
  }
  def getRepository = repository
  def saveRepository: Unit ={
    val output = new ObjectOutputStream(new FileOutputStream(resourcesPath + "repository"))
    output.writeObject(repository)
    output.close()
  }

  def checkCapacityEnoughElseReplace(addCache: CacheMetaData): Unit = {
    if ( (repositorySize + addCache.sizoOfOutputData) > repositoryCapacity){
      replaceCache(addCache.sizoOfOutputData)
    }
    repository.add(addCache)
    repositorySize += addCache.sizoOfOutputData
  }
  /**
   * replace condition: 缓存总大小超过设定阈值；
   * replace algrothom:
   * 1. "use" less, replace first
   * 2. if "use" equal, then less "exeTimeOfDag", replace first
   * 3. if "exeTimeOfDag" equal, then less size of "sizoOfOutputData", replace first
   */
  private def replaceCache( needCacheSize: Double ): Unit = {

    val repo = repository.toArray.asInstanceOf[Array[CacheMetaData]]
                         .sortWith( (x: CacheMetaData, y: CacheMetaData) =>
                                    (x.use < y.use && x.exeTimeOfDag < y.exeTimeOfDag
                                      && x.sizoOfOutputData < y.sizoOfOutputData) ).iterator
    var find = false
    var needCacheSizeCopy = needCacheSize
    while( repo.hasNext && !find ){
      val cache = repo.next()
      if ( cache.sizoOfOutputData >= needCacheSizeCopy ){
        find = true
      }else{
        needCacheSizeCopy -= cache.sizoOfOutputData
      }
      removeCacheFromDisk(cache.outputFilename)
      repositorySize -= cache.sizoOfOutputData
      repository.remove(cache)
    }
  }

  private def removeCacheFromDisk(pathCache: String): Unit = {
    if ( repositoryBasePath.contains("hdfs")){   // delete the hdfs file
      val config = new Configuration()
      val path = new Path(pathCache)
      val hdfs = path.getFileSystem(config)
      hdfs.delete(path, true)
    }else{   // delete the local file
      FileUtils.deleteDirectory(new File(pathCache))
    }
  }

  def fileExist(pathFile: String, fileType: String): Boolean ={
    if ( pathFile.contains("hdfs")){  // hdfs system
      val config = new Configuration()
      val path = new Path(pathFile)
      val hdfs = path.getFileSystem(config)
      if ( !hdfs.exists(path) ){
        removeCacheFromRepository(pathFile, fileType)
        false
      }else{
        true
      }
    }else{  // local file
      if (!(new File(pathFile).exists())){
        removeCacheFromRepository(pathFile, fileType)
        false
      }else{
        true
      }
    }
  }

  private def removeCacheFromRepository(inputFileName: String, fileType: String): Unit = {
    val ite = repository.iterator()
    fileType match {
      case "input" => {
        while ( ite.hasNext){
          val cache = ite.next()
          if ( cache.root.inputFileName.contains(inputFileName)){
            repositorySize -= cache.sizoOfOutputData
            repository.remove(cache)
          }
        }
      }
      case "ouput" => {
        while ( ite.hasNext){
          val cache = ite.next()
          if ( cache.outputFilename.equalsIgnoreCase(inputFileName)){
            repositorySize -= cache.sizoOfOutputData
            repository.remove(cache)
          }
        }
      }
    }
  }

  def getLastModifiedTimeOfFile(filePath: String): Long = {
    var modifiedTime: Long = 0
    if ( filePath.contains("hdfs")){   // hdfs file
      val config = new Configuration()
      val path = new Path(filePath)
      val hdfs = path.getFileSystem(config)
      modifiedTime = hdfs.getFileStatus(path).getModificationTime
    }else{                             // local file
      modifiedTime = (new File(filePath)).lastModified()
    }
    modifiedTime
  }

  def checkFilesNotModified(cacheMetaData: CacheMetaData): Boolean = {
    val inputFileNames = cacheMetaData.root.inputFileName
    val inputFilesLastModifiedTime = cacheMetaData.root.inputFileLastModifiedTime
    inputFileNames.forEach(new Consumer[String] {
      override def accept(t: String): Unit = {
        if ( !CacheManager.getLastModifiedTimeOfFile(t).equals(
          inputFilesLastModifiedTime.get(inputFileNames.indexOf(t)))){
          // consistency maintain
          removeCacheFromRepository(t, "input")
          return false
        }
      }
    })
    // 2. check output files
    val outputFileNotModified = CacheManager.getLastModifiedTimeOfFile(cacheMetaData.outputFilename).equals(cacheMetaData.outputFileLastModifiedTime)
    if ( outputFileNotModified ){
      return true
    }else{
      // consistency maintain
      removeCacheFromDisk(cacheMetaData.outputFilename)
      repositorySize -= cacheMetaData.sizoOfOutputData
      repository.remove(cacheMetaData)
      return false
    }
  }
}