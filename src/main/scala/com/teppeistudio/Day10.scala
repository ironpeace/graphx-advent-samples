package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day10 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val graph = GraphLoader.edgeListFile(sc, "graphdata/day10.tsv").cache()

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		graph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		graph.edges.collect.foreach(println(_))

		// degrees ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		println("\n\n~~~~~~~~~ Confirm inDegrees ")
		graph.inDegrees.collect.foreach(d => println(d._1 + "'s inDegree is " + d._2))

		println("\n\n~~~~~~~~~ Confirm outDegrees ")
		graph.outDegrees.collect.foreach(d => println(d._1 + "'s outDegree is " + d._2))

		println("\n\n~~~~~~~~~ Confirm degrees ")
		graph.degrees.collect.foreach(d => println(d._1 + "'s degree is " + d._2))

		def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
		  if (a._2 > b._2) a else b
		}

		println("\n\n~~~~~~~~~ Confirm max inDegrees ")
		println(graph.inDegrees.reduce(max))

		// collectNeighborIds ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		println("\n\n~~~~~~~~~ Confirm collectNeighborIds(IN) ")
		graph.collectNeighborIds(EdgeDirection.In).collect
			.foreach(n => println(n._1 + "'s in neighbors : " + n._2.mkString(",")))

		println("\n\n~~~~~~~~~ Confirm collectNeighborIds(OUT) ")
		graph.collectNeighborIds(EdgeDirection.Out).collect
			.foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))

		println("\n\n~~~~~~~~~ Confirm collectNeighborIds(Either) ")
		graph.collectNeighborIds(EdgeDirection.Either).collect
			.foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))

		// collectNeighbor ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		println("\n\n~~~~~~~~~ Confirm collectNeighbors(IN) ")
		graph.collectNeighbors(EdgeDirection.In).collect
			.foreach(n => println(n._1 + "'s in neighbors : " + n._2.mkString(",")))

		println("\n\n~~~~~~~~~ Confirm collectNeighbors(OUT) ")
		graph.collectNeighbors(EdgeDirection.Out).collect
			.foreach(n => println(n._1 + "'s out neighbors : " + n._2.mkString(",")))

		println("\n\n~~~~~~~~~ Confirm collectNeighbors(Either) ")
		graph.collectNeighbors(EdgeDirection.Either).collect
			.foreach(n => println(n._1 + "'s neighbors : " + n._2.distinct.mkString(",")))


		sc.stop
	}


}