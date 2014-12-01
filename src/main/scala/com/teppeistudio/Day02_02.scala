package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day02_02 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val vertexLines: RDD[String] = sc.textFile("graphdata/day02-02-vertices.csv")
		val vertices: RDD[(VertexId, (String, Long))]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, (cols(1), cols(2).toLong))
			})

		val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
		val edgeLines: RDD[String] = sc.textFile("graphdata/day02-02-edges.csv")
		val edges:RDD[Edge[((Long, java.util.Date))]]
			= edgeLines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, (cols(2).toLong, format.parse(cols(3))))
			})

		val graph:Graph[(String, Long), (Long, java.util.Date)] = Graph(vertices, edges)

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		graph.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		graph.vertices.collect.foreach(println(_))

		val graph2:Graph[Long, (Long, java.util.Date)]
			= graph.mapVertices((vid:VertexId, attr:(String, Long)) => attr._1.length * attr._2)

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph2 ")
		graph2.vertices.collect.foreach(println(_))

		val graph3:Graph[(String, Long), Long] = graph.mapEdges( edge => edge.attr._1 )

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph3 ")
		graph3.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm triplets Internal of graph ")
		graph.triplets.collect.foreach(println(_))

		val graph4:Graph[(String, Long), Long]
			 = graph.mapTriplets(edge => edge.srcAttr._2 + edge.attr._1 + edge.dstAttr._2)

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph4 ")
		graph4.edges.collect.foreach(println(_))

		val newVertices:VertexRDD[Long] = graph.mapReduceTriplets(
				mapFunc = (edge:EdgeTriplet[(String, Long), (Long, java.util.Date)]) => {
					val toSrc = Iterator((edge.srcId, edge.srcAttr._2 - edge.attr._1))
					val toDst = Iterator((edge.dstId, edge.dstAttr._2 + edge.attr._1))
					toSrc ++ toDst
				},
				reduceFunc = (a1:Long, a2:Long) => ( a1 + a2 )
			)

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of newVertices ")
		newVertices.collect.foreach(println(_))

		sc.stop
	}


}