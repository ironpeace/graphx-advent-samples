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

		val graph2 = graph.mapVertices((id, attr) => List[VertexId]())			// Vertexに空のリストを持たせる
		graph2.vertices.collect.foreach(println(_))

		val graph3 = Pregel(graph2, List[VertexId](), 4, EdgeDirection.Out) (	// 4階層以内で還流しているところを探す
			(id, attr, msg) => (msg ::: attr).distinct, 						// 自分のリストと、渡って来たリストを合体させる
			edge => Iterator((edge.dstId, (edge.srcId +: edge.srcAttr))), 		// Srcが持っているリストにSrcのIDを追加してDstに渡す
			(a, b) => (a ::: b).distinct										// 複数Dstから送られて来たリストを合体させる
		)
		graph3.vertices.collect.foreach(println(_))

		val graph4 = graph3.subgraph(vpred = (id, attr) => attr.contains(id))	// リストに自分のIDが入っているVertexが還流Vertex
		graph4.vertices.collect.foreach(println(_))

		sc.stop
    }

}