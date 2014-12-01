package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day04_01 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val vertexLines: RDD[String] = sc.textFile("graphdata/day04-01-vertices.csv")
		val v: RDD[(VertexId, (String, Long))]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, (cols(1), cols(2).toLong))
			})

		val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
		val edgeLines: RDD[String] = sc.textFile("graphdata/day04-01-edges.csv")
		val e:RDD[Edge[((Long, java.util.Date))]]
			= edgeLines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, (cols(2).toLong, format.parse(cols(3))))
			})

		val graph:Graph[(String, Long), (Long, java.util.Date)] = Graph(v, e)

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		graph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		graph.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Edges reversed graph ")
		graph.reverse.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Subgraphed vertices graph ")
		graph.subgraph(vpred = (vid, v) => v._2 >= 200).vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Subgraphed edges graph ")
		graph.subgraph(epred = edge => edge.attr._1 >= 200).edges.collect.foreach(println(_))

		val subGraph = graph.subgraph(vpred = (vid, v) => v._2 >= 200, epred = edge => edge.attr._1 >= 200)

		println("\n\n~~~~~~~~~ Confirm Subgraphed vertices graph ")
		subGraph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Subgraphed edges graph ")
		subGraph.edges.collect.foreach(println(_))

		val maskedGraph = graph.mask(subGraph)

		println("\n\n~~~~~~~~~ Confirm Masked Graph vertices graph ")
		maskedGraph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Masked Graph edges graph ")
		maskedGraph.edges.collect.foreach(println(_))

		sc.stop
	}


}