package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Graphxsamples {



    def main(args: Array[String]) = {

val conf = new SparkConf()
val sc = new SparkContext("local", "test", conf)
val graph = GraphLoader.edgeListFile(sc, "graphdata/graph_1.tsv").cache()

		// val inNeighbors = graph.collectNeighborIds(EdgeDirection.In).cache()
		// val inNeighborsId = collectNeighborsId(inNeighbors, Array[Long](1), 2)
		// val newEdges = inNeighborsId.collect.flatMap( v => for(n <- v._2) yield (n, v._1) )

		// newEdges.foreach(println(_))

		val ccs = graph.connectedComponents().vertices
		// val ccGrouped = ccs.map(c => (c._2.toInt, c._1.toString)).reduceByKey(_ :: List(_))

		// ccGrouped.collect.foreach(println(_))

		sc.stop
    }

	def collectNeighborsId(	neighbors:VertexRDD[Array[VertexId]],
	    						ids: Array[VertexId],
	    						depth:Int ) : VertexRDD[Array[VertexId]] = {
	        if ( depth == 0 ) {
	            neighbors.filter( v => ids.contains(v._1) )
	        } else {
	            val nextIds = neighbors
	            				.filter( v => ids.contains(v._1) )
	            				.map( v => v._2 )
	            				.reduce( _ ++ _ )
	            collectNeighborsId(neighbors, ids ++ nextIds, depth - 1)
	        }
	}

}