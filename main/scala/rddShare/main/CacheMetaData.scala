package rddShare.main

import java.util.ArrayList

/**
 * Created by hcq on 16-5-5.
 */
class CacheMetaData {

  def this(nodesList: ArrayList[Pair[SimulateRDD, SimulateRDD]], outputFilename: String) {
    this()
    this.nodesList = nodesList
    this.outputFilename = outputFilename
  }

  var nodesList: ArrayList[Pair[SimulateRDD, SimulateRDD]] = null
  var outputFilename: String = null

  var sizoOfInputData: Double = .0
  var sizoOfOutputData: Double = .0
  var exeTimeOfDag: Double = .0

  def getOutputFilename: String = {
    return outputFilename
  }

  def setOutputFilename(outputFilename: String) {
    this.outputFilename = outputFilename
  }

  def getSizoOfInputData: Double = {
    return sizoOfInputData
  }

  def setSizoOfInputData(sizoOfInputData: Double) {
    this.sizoOfInputData = sizoOfInputData
  }

  def getSizoOfOutputData: Double = {
    return sizoOfOutputData
  }

  def setSizoOfOutputData(sizoOfOutputData: Double) {
    this.sizoOfOutputData = sizoOfOutputData
  }

  def getExeTimeOfDag: Double = {
    return exeTimeOfDag
  }

  def setExeTimeOfDag(exeTimeOfDag: Double) {
    this.exeTimeOfDag = exeTimeOfDag
  }
}
