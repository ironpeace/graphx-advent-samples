package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Day99 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val graph = GraphLoader.edgeListFile(sc, "graphdata/day12.tsv").cache()

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		graph.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		graph.edges.collect.foreach(println(_))


		val pg = runUntilConvergence(graph, 1.0)

		println("\n\n~~~~~~~~~ Confirm Vertices of Graph with PageRanl (Tol = 1.0) ")
		pg.vertices.collect.foreach(println(_))

		// graph.pageRank(1.0).vertices.collect.foreach(println(_))

		// println("\n\n~~~~~~~~~ Confirm Vertices of Graph with PageRanl (Tol = 0.0001) ")
		// graph.pageRank(0.0001).vertices.collect.foreach(println(_))


		sc.stop
	}

  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (0.0, 0.0) )
      .cache()


	println("\n\n~~~~~~~~~ Confirm Vertices Internal of initial runUntilConvergence ")
	pagerankGraph.vertices.collect.foreach(println(_))

	println("\n\n~~~~~~~~~ Confirm Edges Internal of initial runUntilConvergence ")
	pagerankGraph.edges.collect.foreach(println(_))


    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)
    println("initialMessage : " + initialMessage)

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank

}