package rddShare.test

import java.util
import java.util.function.Consumer

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import rddShare.core.SimulateRDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hcq on 16-5-13.
 */
object jsonSer {

  def main(args: Array[String]) {

    println()

    val nodesList = new ArrayBuffer[SimulateRDD]
    val index = new util.ArrayList[Integer]()
    for ( i <- 0 to 3){
      val srdd = new SimulateRDD("tran"+i, "function"+i)
      nodesList += srdd
      index.add(i)
    }
    implicit val formats = Serialization.formats(NoTypeHints)
    val js = write(nodesList.toArray[SimulateRDD])
    println(js)
    val cacheCopy = read[Array[SimulateRDD]](js)
    cacheCopy.foreach(println)

    val indexjs = write(index)
    println(indexjs)
    val indexCopy = read[util.ArrayList[Integer]](indexjs)
    indexCopy.forEach(new Consumer[Integer] {
      override def accept(t: Integer): Unit = {
        println(t)
      }
    })
  }

}
