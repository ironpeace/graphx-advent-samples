package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day06 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val vertexLines: RDD[String] = sc.textFile("graphdata/day06-vertices.csv")
		val v: RDD[(VertexId, (String, Long))]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, (cols(1), cols(2).toLong))
			})

		val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
		val edgeLines: RDD[String] = sc.textFile("graphdata/day06-edges.csv")
		val e:RDD[Edge[((Long, java.util.Date))]]
			= edgeLines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, (cols(2).toLong, format.parse(cols(3))))
			})

		val graph:Graph[(String, Long), (Long, java.util.Date)] = Graph(v, e)

		val vertices:VertexRDD[(String, Long)] = graph.vertices

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		vertices.collect.foreach(println(_))

		val edges:EdgeRDD[(Long, java.util.Date), (String, Long)] = graph.edges

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		edges.collect.foreach(println(_))

		sc.stop
	}


}