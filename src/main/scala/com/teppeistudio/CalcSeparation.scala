package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CalcSeparationSample {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)
		val graph = GraphLoader.edgeListFile(sc, "graphdata/calc_separation_sample.tsv").cache()
		//graph.vertices.collect.foreach(println(_))

		// Vertexに空のリストを持たせる
		// List[( 繋がり元のVertex , List[ 繋がり元のVertexと自Vertexの間にいるVertex ])]
		val graph2 = graph.mapVertices((id, attr) => List[(VertexId, List[VertexId])]())
		//graph2.vertices.collect.foreach(println(_))

		// 4階層以内で還流しているところを探す
		val graph3 = Pregel(
				graph2, 
				List[(VertexId, List[VertexId])](), 
				6, // 6次の隔たりに収まるという伝説... 
				EdgeDirection.Out) (

			// 自Vertexで受信したmsgをどう処理するか
			(id, attr, msg) => {
				// 自分のリストと、渡って来たリストをまずは単純に合体させる
				val unioned:List[(VertexId, List[VertexId])] = attr ++ msg
				unioned
					// 繋がり元のVertexでグルーピングする
					.groupBy(_._1)
					// 各「繋がり元のVertex」毎に集約処理を行う
					.map{
						case(k:VertexId, v:List[(VertexId, List[VertexId])]) => {
							v.reduce{ (a:(VertexId,List[VertexId]), b:(VertexId,List[VertexId])) => 
								// 「繋がり元のVertexと自Vertexの間にいるVertex」が少ない方を選択してく
								// つまり隔たりが少ないルートを選択していく
								if(a._2.length < b._2.length) a else b
							}
						}
					}.toList
			},

			// Srcが持っているリストにSrcのIDを追加してDstに渡す
			edge => {
				val newAttr:List[(VertexId, List[VertexId])] = for(a <- edge.srcAttr) yield (a._1, edge.srcId :: a._2)
				Iterator((edge.dstId, (edge.srcId, List(edge.srcId)) :: newAttr))
			},

			// 複数Dstから送られて来たリストを合体させる
			(a, b) => (a ++ b)
		)
		
		//graph3.vertices.collect.foreach(println(_))

		// 隔たり数を集計
		val graph4 = graph3.mapVertices((id:VertexId, attr:List[(VertexId, List[VertexId])]) => {
			for ( a <- attr ) yield (a._1, a._2.length)
		})

		val g4List:Array[(VertexId, List[(VertexId, Int)])] = graph4.vertices.collect
		
		//g4List.foreach(println(_))

		// 辺単位の形に変形
		val eList:Array[List[(String, Int)]] = 
			for(g:(VertexId, List[(VertexId, Int)]) <- g4List) yield {
				for(g2:(VertexId, Int) <- g._2) yield (g2._1 + " -> " + g._1, g2._2)
			}

		val separationsList = eList.flatten
		
		separationsList.foreach(println(_))

		sc.stop
    }

}