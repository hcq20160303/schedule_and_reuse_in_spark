package rddShare.tool

import java.io._

import rddShare.core.RDDShare

import scala.io.Source
import scala.sys.process.Process
/**
 * Created by hcq on 16-5-7.
 */
object MyUtils {
  def getFunctionOfRDD(input: InputStream, funClassPath: String): String ={
    var function = ""
    val pathOfFunctionJava = RDDShare.getAnnoFunctionCopyPath+funClassPath+".class"
    // copy the .class file to pathOfFunctionClass
    MyFileIO.fileWriteBytes(pathOfFunctionJava, MyFileIO.fileReadBytes(input))
    // recomplie the .class file to .java file use jad
    val command = "jad -sjava " + pathOfFunctionJava
    Process(command).!

    val pathOfJavaFile = pathOfFunctionJava.split(".cla")(0) + ".java"
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

  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

}
