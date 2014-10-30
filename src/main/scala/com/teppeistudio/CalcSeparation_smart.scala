package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CalcSeparationSample_smart {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		// vertexのattrに、自分の他Vertexとその距離のタプル型リストを持つ、グラフを生成する
		val graph = GraphLoader
						.edgeListFile(sc, "graphdata/calc_separation_sample.tsv")
						.mapVertices((id:VertexId, attr:Int) => List((id, 0)))
						.cache()

		//graph.vertices.collect.foreach(println(_))

		// 短い方の距離を選定
		def minRouteDepth(v1:(VertexId, Int), v2:(VertexId, Int))
			= if(v1._2 < v2._2) v1 else v2

		// Vertex毎が持つ、他Vertexとの距離情報の集約
		def mergeVertexRoute(oldRoute:List[(VertexId, Int)], newRoute:List[(VertexId, Int)])
		 	= (oldRoute ++ newRoute)
		 		//VertexIdでグルーピング
		 		.groupBy(_._1)
		 		//VertexIdでグルーピングされた中で距離の短い方を選択
		 		.map(_._2.reduce((v1, v2) => minRouteDepth(v1, v2)))
		 		.toList

		val graph2
			= Pregel(
				graph, 
				//最初に流すメッセージは空のリスト
				List[(VertexId, Int)](),
				//半永久的Loop
				Int.MaxValue,
				EdgeDirection.Out
				)(
				// 各Vertexでは、送られて来たMSG内にある、他Vertexとその距離に対して、１を足して行く
				// そして、自分が持つ、自分の他Vertexとその距離のタプル型リストに対して、
				// 他Vertex毎の距離情報を集約していく。
				// 集約していく際に、距離の短い方を選択する。
				(id, attr, msg) => mergeVertexRoute(attr, msg.map(a=> (a._1, a._2 + 1))),
				edge => {
					// srcVertexが持つ、他Vertexとの距離リストの中に、
					// dstVertexのvertexIDがあるかをチェックし、
					// あるようであれば、循環していると判断する
					val isCyclic = edge.srcAttr.filter(_._1 == edge.dstId).nonEmpty
					// 循環しているようであれば、既に距離は計測済みなので、
					// このEdgeに関してはもうMSG送信しない。
					// さもなければ、dstVertexに対して、srcVertexが持つ、
					// 他Vertexとその距離のリストを送る
					if(isCyclic) Iterator.empty else Iterator((edge.dstId, edge.srcAttr))
				},
				// 複数のsrcから送られて来たリストは単純合体させる
				(m1, m2) => m1 ++ m2
			)

		graph2.vertices.collect().foreach(v => 
			v._2.filter(_._2 != 0).foreach(e =>
				println(e._1 + " -> " + v._1 + " , " + e._2)
			)
		)

		sc.stop
    }

}