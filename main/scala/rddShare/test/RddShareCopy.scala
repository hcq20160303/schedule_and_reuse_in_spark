package rddShare.test

import java.sql.{ResultSet, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.Properties

/**
 * Created by hcq on 16-4-19.
 */

object RddShareCopy {

  private val conf = new SparkConf().setAppName("RDDShareCopy")
    .setMaster("spark://192.168.1.105:7077")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "/home/hcq/Documents/spark_1.5.0/eventLog")
  private val sc = new SparkContext(conf)

  def main(args: Array[String]) {

//    connectMysql
    println(Properties.envOrElse("SPARK_HOME", "/home/hcq/Desktop/spark"))
    val re = sc.objectFile("/home/hcq/Documents/spark_1.5.0/repository/8969451351462891627711reduceByKey")
    re.foreach(println)
  }

  def connectMysql(): Unit ={
    val conn_str = "jdbc:mysql://localhost:3306/rddShare?user=root"
    // Load the driver
    classOf[com.mysql.jdbc.Driver]

    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM repository")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getString("outputFilename"))
      }
    }
    finally {
      conn.close
    }
  }

  def stopSpark() {
    sc.stop()
  }
}
