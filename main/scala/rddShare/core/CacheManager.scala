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
import scala.tools.nsc.Properties
import scala.util.parsing.json.JSON

/**
 * Created by hcq on 16-5-9.
 */
object CacheManager {



  val confPath = Properties.envOrElse("SPARK_HOME", "/home/hcq/Desktop/spark_1.5.0")
  val conf = ConfigFactory.parseFile(new File(confPath + "/conf/rddShare/default.conf"))
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

    val jsonLines = Source.fromFile(confPath + "/conf/rddShare/transformation.json").getLines()
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
  val com = new Comparator[CacheMetaData]() with Serializable{
    /**
     * 排序规则：
     * 1. dag树的节点数量越多越靠前
     * 2. “加载数据”操作符（Scan）越多，则越靠前
     * 3. filename
     * 4. 操作符优先级
     * 为什么需要排序？是为了保证第一次匹配成功的dag就是最大匹配
     */
    def compare(o1: CacheMetaData, o2: CacheMetaData): Int = {
      if (o1.nodesList.length > o2.nodesList.length ) {       // rule 1. dag树的节点数量越多越靠前
        return -1
      }
      else if (o1.nodesList.length < o2.nodesList.length ) {
        return 1
      }
      else {
        val o1inputFilenames: ArrayList[String] = o1.root.inputFileName
        val o2inputFilenames: ArrayList[String] = o2.root.inputFileName
        if (o1inputFilenames.size > o2inputFilenames.size) {   // rule 2. “加载数据”操作符（Scan）越多，则越靠前
          return -1
        }
        else if (o1inputFilenames.size < o2inputFilenames.size) {
          return 1
        }
        else {
          var compare: Int = 0
          for( i <- 0 to o1inputFilenames.size-1){   // rule 3. filename
            compare = o1inputFilenames.get(i).compareToIgnoreCase(o2inputFilenames.get(i))
            if ( compare != 0 ){
              return -compare
            }
          }

          val o1allTransformation: ArrayList[String] = o1.root.allTransformation
          val o2allTransformation: ArrayList[String] = o2.root.allTransformation
          for ( i <- 0 to o1allTransformation.size-1) {   // rule 4. allTransoformation
            compare = TRANSFORMATION_PRIORITY.get(o1allTransformation.get(i)) - TRANSFORMATION_PRIORITY.get(o2allTransformation.get(i))
            if (compare != 0) {
              return -compare
            }
          }
          return 0
        }
      }
    }
  }
  private val repository: TreeSet[CacheMetaData] = new TreeSet[CacheMetaData](com)
  def initRepository: Unit = {
    // load the history cacheMetaData to repository from disk if rddShare system has the history data
    if (Files.exists(Paths.get(confPath+"/conf/rddShare/repository"))){
      val input = new ObjectInputStream(new FileInputStream(confPath+"/conf/rddShare/repository"))
      val repo = input.readObject().asInstanceOf[TreeSet[CacheMetaData]]
      input.close()
      val ite = repo.iterator()
      while (ite.hasNext){
        val cache = ite.next()
        repository.add(cache)
        repositorySize += cache.sizeOfOutputData
      }
    }
    println("CacheManager.scala---initRepository")
    println("repositorySize: "+ repositorySize + "\trepository.size(): " + repository.size())
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
        "sizoOfOutputData: " + t.sizeOfOutputData+ "\tuse: " + t.reuse)
      }
    })
  }
  def getRepository = repository
  def saveRepository: Unit ={
    val output = new ObjectOutputStream(new FileOutputStream(confPath+"/conf/rddShare/repository"))
    output.writeObject(repository)
    output.close()
    println("CacheManager.scala---saveRepository")
    println("repositorySize: "+ repositorySize + "\trepository.size(): " + repository.size())
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
          "sizoOfOutputData: " + t.sizeOfOutputData + "\tuse: " + t.reuse)
      }
    })
  }

  def checkCapacityEnoughElseReplace(addCache: CacheMetaData): Unit = {
    if ( addCache.sizeOfOutputData > repositoryCapacity ){
      println("CacheManager.scala---checkCapacityEnoughElseReplace")
      println("sizoOfOutputData is bigger than repositoryCapacity, we suggest you adjust the repositoryCapacity or don't cache this guy")
    }
    if ( (repositorySize + addCache.sizeOfOutputData) > repositoryCapacity){
      println("CacheManager.scala---checkCapacity: (repositorySize: " + repositorySize +
        " + addCache.sizoOfOutputData: " + addCache.sizeOfOutputData + ") > repositoryCapacity: " + repositoryCapacity)
      replaceCache(addCache.sizeOfOutputData)
    }
    repository.add(addCache)
    repositorySize += addCache.sizeOfOutputData
    println("CacheManager.checkCapacityEnoughElseReplace: repository contents")
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
          "sizoOfOutputData: " + t.sizeOfOutputData + "\tuse: " + t.reuse)
      }
    })
  }
  /**
   * replace condition: 缓存总大小超过设定阈值；
   * replace algrothom:
   * 1. "use" less, replace first
   * 2. if "use" equal, then less "exeTimeOfDag", replace first
   * 3. if "exeTimeOfDag" equal, then less size of "sizoOfOutputData", replace first
   */
  private def replaceCache( needCacheSize: Double ): Unit = {
    // copy a repository to re-sorted
    val repoCopy = new TreeSet[CacheMetaData]( new Comparator[CacheMetaData]() with Serializable{
      /**
       * rules of sort:
       * 1. less use, near the front more
       * 2. less exeTimeOfDag, near the front more
       * 3. less sizeOfOutputData, near the front more
       * 4. less nodes, near the front more
       * 5. less Scan, near the front more
       * 6. small outputFilename, near the front more
       * why we need sorted? because we can get the less useful CacheMetaData first
       */
      def compare(o1: CacheMetaData, o2: CacheMetaData): Int = {
        if (o1.reuse < o2.reuse ) {       // rule 1
          return -1
        }
        else if (o1.reuse > o2.reuse ) {
          return 1
        }
        else {
          if (o1.exeTimeOfDag < o2.exeTimeOfDag) {   // rule 2
            return -1
          }
          else if (o1.exeTimeOfDag > o2.exeTimeOfDag) {
            return 1
          }
          else {
            if ( o1.sizeOfOutputData < o2.sizeOfOutputData ){  // rule 3
              return -1
            }else if ( o1.sizeOfOutputData > o2.sizeOfOutputData ){
              return 1
            }else {
              if ( o1.nodesList.length < o2.nodesList.length){  // rule 4
                return -1
              }else if ( o1.nodesList.length > o2.nodesList.length){
                return 1
              }else{
                if ( o1.indexOfDagScan.size() < o2.indexOfDagScan.size() ){  // rule 5
                  return -1
                }else if ( o1.indexOfDagScan.size() > o2.indexOfDagScan.size() ){
                  return 1
                }else{
                  return o1.outputFilename.compare(o2.outputFilename)   // rule 5
                }
              }
            }
          }
        }
      }
    })
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        repoCopy.add(t)
      }
    })
    var repo = repoCopy.iterator()
    println("CacheManager.scala---replaceCache---sorted repository: ")
    repo.forEachRemaining(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        println(t.toString)
      }
    })
    // after println, we must re-get the iterator
    repo = repoCopy.iterator()
    var find = false
    var needCacheSizeCopy = needCacheSize
    while( repo.hasNext && !find ){
      val cache = repo.next()
      if ( cache.sizeOfOutputData >= needCacheSizeCopy ){
        find = true
      }
      needCacheSizeCopy -= cache.sizeOfOutputData
      removeCacheFromDisk(cache.outputFilename)
      repositorySize -= cache.sizeOfOutputData
      repository.remove(cache)
    }
    if ( needCacheSizeCopy > 0 ){
      println("CacheManager.scala---replaceCache")
      println("needCacheSize is bigger than repositorySize, so the repository only left this guy")
    }
  }

  private def removeCacheFromDisk(pathCache: String): Unit = {
    println("CacheManager.scala---removeCacheFromDisk's remove path: " + pathCache)
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
            repositorySize -= cache.sizeOfOutputData
            repository.remove(cache)
          }
        }
      }
      case "ouput" => {
        while ( ite.hasNext){
          val cache = ite.next()
          if ( cache.outputFilename.equalsIgnoreCase(inputFileName)){
            repositorySize -= cache.sizeOfOutputData
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
      repositorySize -= cacheMetaData.sizeOfOutputData
      repository.remove(cacheMetaData)
      return false
    }
  }
}