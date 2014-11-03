package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random

object CreateSampleGraph {

    def main(args : Array[String]) = {

	    if (args.length < 1) {
	      System.err.println("Usage: CreateSampleGraph <vertexCount> <friendCount>")
	      System.exit(1)
	    }

	    val vertexCount:Long = args(0).toLong
	    val friendCount:Int = args(1).toInt

		val conf = new SparkConf().setAppName("CreateSampleGraph")
		val sc = new SparkContext(conf)
		// val sc = new SparkContext("local", "test", conf)

		val edgeList:RDD[(Long, Long)] = sc.parallelize(createRandomEdge(vertexCount, friendCount))
		// val edgeList:RDD[(Long, Long)] = sc.parallelize(createRandomEdge(1000000L, 10))
		val edges: RDD[Edge[Long]] = edgeList.map(e => Edge(e._1, e._2, 0L))
		val graph = Graph.fromEdges(edges, VertexRDD(sc.parallelize(List((1L,0)))))

		println("~~~~~~~~~~~~~ " + graph.edges.count + " Edges")

		val distances = getDistanceList(graph)

		println("~~~~~~~~~~~~~ Distances")
		distances.toList.sortBy(s => s._1).foreach(println(_))

		sc.stop
    }

    def createRandomEdge(vertexLen:Long, friendsNum:Int):List[(Long, Long)] = {
		val r = new Random()
		val list = (1L to vertexLen).toList

		val ret:List[List[(Long, Long)]]
		 = for(i <- list) yield {
			val rlist = r.shuffle(list).filterNot(_ == i)
			(for(d <- 1 to friendsNum) yield (i, rlist(d))).toList
		}.toList

		ret.flatten
    }

    def getDistanceList(graph:Graph[VertexRDD[Int], Long]):scala.collection.Map[Int,Long] = {

		def minRouteDepth(v1:(VertexId, Int), v2:(VertexId, Int))
			= if(v1._2 < v2._2) v1 else v2

		def mergeVertexRoute(oldRoute:List[(VertexId, Int)], newRoute:List[(VertexId, Int)])
		 	= (oldRoute ++ newRoute)
		 		.groupBy(_._1)
		 		.map(_._2.reduce((v1, v2) => minRouteDepth(v1, v2)))
		 		.toList

		val graph2
			= Pregel(
				graph.mapVertices((id:VertexId, attr:VertexRDD[Int]) => List((id, 0))),
				List[(VertexId, Int)](),
				Int.MaxValue,
				EdgeDirection.Out
				)(
				(id, attr, msg) => mergeVertexRoute(attr, msg.map(a=> (a._1, a._2 + 1))),
				edge => {
					val isCyclic = edge.srcAttr.filter(_._1 == edge.dstId).nonEmpty
					if(isCyclic) Iterator.empty else Iterator((edge.dstId, edge.srcAttr))
				},
				(m1, m2) => m1 ++ m2
			)

		graph2.vertices.flatMap(v => v._2).map(m => (m._2, m._1)).countByKey
    }

}