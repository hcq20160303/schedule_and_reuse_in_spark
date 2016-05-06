package rddShare.tool

import java.io.{FileOutputStream, InputStream}

import org.apache.commons.io.IOUtils

/**
 * Created by hcq on 16-4-27.
 */
object MyFileIO {

  def fileWriteBytes(path: String, bytes: Array[Byte]): Unit ={
    val fileoutputStream = new FileOutputStream(path)
    IOUtils.write(bytes, fileoutputStream)
  }

  def fileReadBytes(input: InputStream): Array[Byte] ={
    Stream.continually(input.read()).takeWhile(-1 != ).map(_.toByte).toArray
  }

}
