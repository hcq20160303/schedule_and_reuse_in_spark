package rddShare.main

import java.util.ArrayList

import org.apache.spark.rdd.RDD

/**
 * Created by hcq on 16-5-5.
 */
object RDDShare {

  def transformDAGtoList(finalRDD: RDD): ArrayList[Pair[SimulateRDD, SimulateRDD]] ={
    var nodesList = new ArrayList[Pair[SimulateRDD, SimulateRDD]]

    /**
     * your code in here
     */

    nodesList
  }

}
