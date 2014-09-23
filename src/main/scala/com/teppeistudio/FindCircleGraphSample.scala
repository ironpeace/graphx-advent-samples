package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object FindCircleGraphSample {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)
		val graph = GraphLoader.edgeListFile(sc, "graphdata/find_circle_graph_sample.tsv").cache()
		graph.vertices.collect.foreach(println(_))

		// Vertexに空のリストを持たせる
		val graph2 = graph.mapVertices((id, attr) => Set[VertexId]())
		graph2.vertices.collect.foreach(println(_))

		// 4階層以内で還流しているところを探す
		val graph3 = Pregel(graph2, Set[VertexId](), 4, EdgeDirection.Out) (
			// 自分のリストと、渡って来たリストを合体させる
			(id, attr, msg) => (msg ++ attr),
			// Srcが持っているリストにSrcのIDを追加してDstに渡す
			edge => Iterator((edge.dstId, (edge.srcAttr + edge.srcId))),
			// 複数Dstから送られて来たリストを合体させる
			(a, b) => (a ++ b)		
		)
		graph3.vertices.collect.foreach(println(_))

		val graph4 = graph3.subgraph(vpred = (id, attr) => attr.contains(id))	// リストに自分のIDが入っているVertexが還流Vertex
		graph4.vertices.collect.foreach(println(_))

		sc.stop
    }

}