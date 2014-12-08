package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day09 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val vertexLines: RDD[String] = sc.textFile("graphdata/day09-vertices.csv")
		val v: RDD[(VertexId, (String, Long))]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, (cols(1), cols(2).toLong))
			})

		val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
		val edgeLines: RDD[String] = sc.textFile("graphdata/day09-01-edges.csv")
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

		// reverse ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		println("\n\n~~~~~~~~~ Confirm Edges reversed graph ")
		graph.reverse.edges.collect.foreach(println(_))

		// subgraph ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		println("\n\n~~~~~~~~~ Confirm Subgraphed vertices graph ")
		graph.subgraph(vpred = (vid, v) => v._2 >= 200).vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Subgraphed edges graph ")
		graph.subgraph(epred = edge => edge.attr._1 >= 200).edges.collect.foreach(println(_))

		val subGraph = graph.subgraph(vpred = (vid, v) => v._2 >= 200, epred = edge => edge.attr._1 >= 200)

		println("\n\n~~~~~~~~~ Confirm vertices of Subgraphed graph ")
		subGraph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm edges of Subgraphed graph ")
		subGraph.edges.collect.foreach(println(_))

		// mask ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		val maskedGraph = graph.mask(subGraph)

		println("\n\n~~~~~~~~~ Confirm Masked Graph vertices graph ")
		maskedGraph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Masked Graph edges graph ")
		maskedGraph.edges.collect.foreach(println(_))

		// groupEdge ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		val edgeLines2: RDD[String] = sc.textFile("graphdata/day09-02-edges.csv")
		val e2:RDD[Edge[((Long, java.util.Date))]]
			= edgeLines2.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, (cols(2).toLong, format.parse(cols(3))))
			})

		val graph2:Graph[(String, Long), (Long, java.util.Date)] = Graph(v, e2)

		val edgeGroupedGraph:Graph[(String, Long), (Long, java.util.Date)]
			= graph2.groupEdges(merge = (e1, e2) => (e1._1 + e2._1, if(e1._2.getTime < e2._2.getTime) e1._2 else e2._2))

		println("\n\n~~~~~~~~~ Confirm merged edges graph ")
		edgeGroupedGraph.edges.collect.foreach(println(_))

		sc.stop
	}


}