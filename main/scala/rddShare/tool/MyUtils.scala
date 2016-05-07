package rddShare.tool

import java.io.InputStream

import rddShare.main.RDDShare

import scala.io.Source
import scala.sys.process.Process

/**
 * Created by hcq on 16-5-7.
 */
object MyUtils {
  def getFunctionOfRDD(input: InputStream, funClassPath: String): String ={
    var function = ""
    val pathOfFunctionClass = RDDShare.getBasePath+funClassPath+".class"
    // copy the .class file to pathOfFunctionClass
    MyFileIO.fileWriteBytes(pathOfFunctionClass, MyFileIO.fileReadBytes(input))
    // recomplie the .class file to .java file use jad
    Process("jad ").!

    val pathOfJavaFile = pathOfFunctionClass.split(".cla")(0) + ".java"
    val sources = Source.fromFile(pathOfJavaFile).getLines().toIterator
    var findApply = false
    while ( sources.hasNext ){
      val line = sources.next()
      if ( line.contains("apply") ){
        findApply = true
      }
      if ( findApply ) {
        function += line
      }
    }
    function
  }
}
