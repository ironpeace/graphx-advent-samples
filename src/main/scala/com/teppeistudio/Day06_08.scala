package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day06_08 {

    def main(args: Array[String]) = {

    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    	// Day06 : VertexRDD と EdgeRDD とは
    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    	// Day07 : VertexRDD
    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		val filteredVertices:VertexRDD[(String, Long)]
			= vertices.filter{ case (vid:VertexId, (name:String, value:Long)) => value > 150 }

		println("\n\n~~~~~~~~~ Confirm filtered vertices ")
		filteredVertices.collect.foreach(println(_))

		val mappedVertices:VertexRDD[Long]
			= vertices.mapValues((vid:VertexId, attr:(String, Long)) => attr._2 * attr._1.length)

		println("\n\n~~~~~~~~~ Confirm mapped vertices ")
		mappedVertices.collect.foreach(println(_))


		println("\n\n~~~~~~~~~ Confirm diffed vertices ")
		// val diffedVertices:VertexRDD[(String, Long)] = filteredVertices.diff(vertices)
		val diffedVertices:VertexRDD[(String, Long)] = vertices.diff(filteredVertices)

		// diffedVertices.collect.foreach(println(_))
		println("vertices : " + vertices.count)
		println("filteredVertices : " + filteredVertices.count)
		println("diffedVertices : " + diffedVertices.count)

		val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 2L).map(id => (id, id.toInt)))
		println("\n\n~~~~~~~~~ set A ")
		setA.collect.foreach(println(_))
		val setB: VertexRDD[Int] = VertexRDD(sc.parallelize(1L until 3L).map(id => (id, id.toInt)))
		println("\n\n~~~~~~~~~ set B ")
		setB.collect.foreach(println(_))
		val diff = setA.diff(setB)
		println("\n\n~~~~~~~~~ diff ")
		diff.collect.foreach(println(_))

		val verticesWithCountry: RDD[(VertexId, String)]
			= sc.textFile("graphdata/day07-01-vertices.csv").map(line => {
				(line.split(",")(0).toLong, line.split(",")(1))
			})

		val leftJoinedVertices = vertices.leftJoin(verticesWithCountry){
			(vid, left, right) => (left._1, left._2, right.getOrElse("World"))
		}

		println("\n\n~~~~~~~~~ Confirm leftJoined vertices ")
		leftJoinedVertices.collect.foreach(println(_))

		val innerJoinedVertices = vertices.innerJoin(verticesWithCountry){
			(vid, left, right) => (left._1, left._2, right)
		}

		println("\n\n~~~~~~~~~ Confirm innerJoined vertices ")
		innerJoinedVertices.collect.foreach(println(_))

		val verticesWithNum: RDD[(VertexId, Long)]
			= sc.textFile("graphdata/day07-02-vertices.csv").map(line => {
				(line.split(",")(0).toLong, line.split(",")(1).toLong)
			})

		val auiVertices:VertexRDD[Long] = vertices.aggregateUsingIndex(verticesWithNum, _ + _)
		
		println("\n\n~~~~~~~~~ Confirm aggregateUsingIndexed vertices ")
		auiVertices.collect.foreach(println(_))

    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    	// Day08 : EdgeRDD とは
    	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		val mappedEdges:EdgeRDD[Long, (String, Long)] = edges.mapValues(edge => edge.attr._1 + 1)

		println("\n\n~~~~~~~~~ Confirm mapped edges ")
		mappedEdges.collect.foreach(println(_))


		val reversedEdges:EdgeRDD[(Long, java.util.Date), (String, Long)] = edges.reverse

		println("\n\n~~~~~~~~~ Confirm reversed edges ")
		reversedEdges.collect.foreach(println(_))


		val e2:RDD[Edge[String]]
			= sc.textFile("graphdata/day08-edges.csv").map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, cols(2))
			})

		val graph2:Graph[(String, Long), String] = Graph(v, e2)
		val edges2 = graph2.edges
		val innerJoinedEdge:EdgeRDD[(Long, java.util.Date, String), (String, Long)]
		 = edges.innerJoin(edges2)((v1, v2, attr1, attr2) => (attr1._1, attr1._2, attr2))

		println("\n\n~~~~~~~~~ Confirm innerJoined edge ")
		innerJoinedEdge.collect.foreach(println(_))


		sc.stop
	}


}