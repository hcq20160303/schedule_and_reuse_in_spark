package rddShare.core

import java.io._
import java.sql.{DriverManager, ResultSet}
import java.util
import java.util._
import java.util.function.Consumer

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.io.Source
import scala.tools.nsc.Properties

/**
 * Created by hcq on 16-5-9.
 */
object CacheManager {

  val confPath = Properties.envOrElse("SPARK_HOME", "/home/hcq/Desktop/spark_1.5.0")
  val conf = ConfigFactory.parseFile(new File(confPath + "/conf/rddShare/default.conf"))

  // Setup the database connection
  val conn = {
    val database = conf.getString("rddShare.database")
    val databaseName = conf.getString("rddShare.databaseName")
    val user = conf.getString("rddShare.databaseUser")
    val conn_str = "jdbc:" + database + "/" + databaseName + "?user=" + user
    // Load the driver
    if (database.contains("mysql")){
      // mysql dirver
      classOf[com.mysql.jdbc.Driver]
    }else{
      // other driver
    }
    DriverManager.getConnection(conn_str)
  }

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

    val jsonLines = Source.fromFile(confPath + "/conf/rddShare/transformation").getLines()
    jsonLines.foreach( line => tranformtion_priority.put(line.split(" ")(0), line.split(" ")(1).toInt ))

    tranformtion_priority
  }
  def tranformtion_priority = TRANSFORMATION_PRIORITY
  val comp = new Comparator[CacheMetaData]() with Serializable{
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
  private val repository: TreeSet[CacheMetaData] = new TreeSet[CacheMetaData](comp)

  def initRepository: Unit = {
    // load the history cacheMetaData to repository from disk if rddShare system has the history data
//    if (Files.exists(Paths.get(confPath+"/conf/rddShare/repository"))){
//      val input = new ObjectInputStream(new FileInputStream(confPath+"/conf/rddShare/repository"))
//      val repo = input.readObject().asInstanceOf[TreeSet[CacheMetaData]]
//      input.close()
//      val ite = repo.iterator()
//      while (ite.hasNext){
//        val cache = ite.next()
//        repository.add(cache)
//        repositorySize += cache.sizeOfOutputData
//      }
//    }

    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM repository")
      // Iterate Over ResultSet
      while (rs.next) {
        val id = rs.getInt("id")
        // deserilize the nodesList & indexofDagScan
        implicit val formats = Serialization.formats(NoTypeHints)
        val nodesList = read[Array[SimulateRDD]](rs.getString("nodesList"))
        val indexOfDagScan = read[util.ArrayList[Integer]](rs.getString("indexOfDagScan"))
        val outputFileLastModifiedTime = rs.getDouble("outputFileLastModifiedTime")
        val sizeOfOutputData = rs.getDouble("sizeOfOutputData")
        val exeTimeOfDag = rs.getDouble("exeTimeOfDag")
        val reuse = rs.getInt("reuse")
        val outputFilename = rs.getString("outputFilename")
        val insertTime = rs.getTimestamp("insertTime")

        val cache = new CacheMetaData(id, nodesList, indexOfDagScan, outputFilename, outputFileLastModifiedTime,
                                      sizeOfOutputData, exeTimeOfDag, insertTime)
        cache.reuse = reuse
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
  private def reinitRepositoryfromDatabase = {
    repository.clear()
    initRepository
  }
//  def saveRepository: Unit ={
//    val output = new ObjectOutputStream(new FileOutputStream(confPath+"/conf/rddShare/repository"))
//    output.writeObject(repository)
//    output.close()
//    println("CacheManager.scala---saveRepository")
//    println("repositorySize: "+ repositorySize + "\trepository.size(): " + repository.size())
//    repository.forEach(new Consumer[CacheMetaData] {
//      override def accept(t: CacheMetaData): Unit = {
//        println("nodesList(0).inputFileName:" + t.nodesList(0).inputFileName + "\t" +
//          "sizoOfOutputData: " + t.sizeOfOutputData + "\tuse: " + t.reuse)
//      }
//    })
//  }

  def checkCapacityEnoughElseReplace(addCache: CacheMetaData): Unit = {

    // we must reget repository from database
    reinitRepositoryfromDatabase

    if ( addCache.sizeOfOutputData > repositoryCapacity ){
      println("CacheManager.scala---checkCapacityEnoughElseReplace")
      println("sizoOfOutputData is bigger than repositoryCapacity, we suggest you adjust the repositoryCapacity or don't cache this guy")
    }
    if ( (repositorySize + addCache.sizeOfOutputData) > repositoryCapacity){
      println("CacheManager.scala---checkCapacity: (repositorySize: " + repositorySize +
        " + addCache.sizoOfOutputData: " + addCache.sizeOfOutputData + ") > repositoryCapacity: " + repositoryCapacity)
      replaceCache(addCache.sizeOfOutputData)
    }
    insertIntoDatabase(addCache)
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
    val calendar = Calendar.getInstance()
    val now = calendar.getTime()
    val current = new java.sql.Timestamp(now.getTime())
    // remove the cache in seven days ago
    repository.forEach(new Consumer[CacheMetaData] {
      override def accept(t: CacheMetaData): Unit = {
        if ( (current.getTime - t.insertTime.getTime).toDouble/(1000*3600*24) >= 7 ){
          deletefromDatabase(t)
          repositorySize -= t.sizeOfOutputData
          repository.remove(t)
        }
      }
    })
    if ( repositorySize + needCacheSize > repositoryCapacity ){
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
        deletefromDatabase(cache)
        repositorySize -= cache.sizeOfOutputData
        repository.remove(cache)
      }
      if ( needCacheSizeCopy > 0 ){
        println("CacheManager.scala---replaceCache")
        println("needCacheSize is bigger than repositorySize, so the repository only left this guy")
      }
    }
  }

  private def insertIntoDatabase(addCache: CacheMetaData): Unit ={
    val id = 0
    implicit val formats = Serialization.formats(NoTypeHints)
    val nodesList = write(addCache.nodesList)
    val indexOfDagScan = write(addCache.indexOfDagScan)
    val outputFileLastModifiedTime = addCache.outputFileLastModifiedTime
    val sizeOfOutputData = addCache.sizeOfOutputData
    val exeTimeOfDag = addCache.exeTimeOfDag
    val reuse = addCache.reuse
    val outputFilename = addCache.outputFilename

    // 1) create a java calendar instance
    val calendar = Calendar.getInstance()
    // 2) get a java.util.Date from the calendar instance.
    //    this date will represent the current instant, or "now".
    val now = calendar.getTime()
    // 3) a java current time (now) instance
    val insertTime = new java.sql.Timestamp(now.getTime())
    val statement = conn.createStatement()
    statement.executeUpdate(s"insert into repository values($id, '$nodesList', '$indexOfDagScan', $outputFileLastModifiedTime," +
      s"$sizeOfOutputData, $exeTimeOfDag, $reuse, '$outputFilename', '$insertTime')")

  }

  private def deletefromDatabase(addCache: CacheMetaData): Unit ={
    val deleteId = addCache.id
    val statement = conn.createStatement()
    statement.executeUpdate(s"delete from repository where id = $deleteId")
  }

  def updatefromDatabase(sql: String): Unit ={
    val statement = conn.createStatement()
    statement.executeUpdate(sql)
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
            deletefromDatabase(cache)
            repositorySize -= cache.sizeOfOutputData
            repository.remove(cache)
          }
        }
      }
      case "ouput" => {
        while ( ite.hasNext){
          val cache = ite.next()
          if ( cache.outputFilename.equalsIgnoreCase(inputFileName)){
            deletefromDatabase(cache)
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
          println("CacheManager.getLastModifiedTimeOfFile(t)): " + CacheManager.getLastModifiedTimeOfFile(t))
          println("inputFilesLastModifiedTime.get(inputFileNames.indexOf(t)): " + inputFilesLastModifiedTime.get(inputFileNames.indexOf(t)))
          // consistency maintain
          removeCacheFromRepository(t, "input")
          return false
        }
      }
    })
    // 2. check output files
    println("CacheManager.getLastModifiedTimeOfFile(cacheMetaData.outputFilename: " + CacheManager.getLastModifiedTimeOfFile(cacheMetaData.outputFilename))
    println("cacheMetaData.outputFileLastModifiedTime): " + cacheMetaData.outputFileLastModifiedTime)

    val outputFileNotModified = CacheManager.getLastModifiedTimeOfFile(cacheMetaData.outputFilename).equals(cacheMetaData.outputFileLastModifiedTime)
    if ( outputFileNotModified ){
      return true
    }else{
      // consistency maintain
      removeCacheFromDisk(cacheMetaData.outputFilename)
      deletefromDatabase(cacheMetaData)
      repositorySize -= cacheMetaData.sizeOfOutputData
      repository.remove(cacheMetaData)
      return false
    }
  }
}