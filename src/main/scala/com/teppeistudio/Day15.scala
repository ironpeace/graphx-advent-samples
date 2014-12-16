package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Day15 {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    val graph:Graph[Int, Int]
    	= GraphLoader.edgeListFile(sc, "graphdata/day15.tsv").cache()

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))

	// 短い方􏰁距離を選定
	def minRouteDepth(v1:(VertexId, Int), v2:(VertexId, Int))  = if(v1._2 < v2._2) v1 else v2

	// 頂点毎に持つ、他頂点と􏰁距離情報􏰁集約
	def mergeVertexRoute(oldRoute:List[(VertexId, Int)], newRoute:List[(VertexId, Int)])
		= (oldRoute ++ newRoute)
		//頂点Idでグルーピング
		.groupBy(_._1)
		//頂点Idでグルーピングされた中で距離􏰁短い方を選択   
		.map(_._2.reduce((v1, v2) => minRouteDepth(v1, v2)))
		.toList

	val graphWithDistance = Pregel(
		// 他頂点􏰁距離􏰁リストを頂点に持たせる
		graph.mapVertices((id:VertexId, attr:Int) => List((id, 0))), List[(VertexId, Int)](), 
		//最初に流すメッセージ空􏰁リスト
		Int.MaxValue,
		// イテレーション回数􏰂指定しない
		EdgeDirection.Out
		// Out方向にメッセージ送信する
	)(
		// 各頂点で􏰂送られて来たメッセージ内にある他頂点とそ􏰁距離に対して1を足して行き、 
		// それと自分が持つ情報とを、距離が短い方を選択しつつ集約していく。
		(id, attr, msg) => mergeVertexRoute(attr, msg.map(a=> (a._1, a._2 + 1))),
		edge => {
			// srcが持つ他頂点と􏰁距離リスト􏰁中に、dst􏰁頂点IDがあれ􏰃循環していると判断
			val isCyclic = edge.srcAttr.filter(_._1 == edge.dstId).nonEmpty
			// 循環しているようであれ􏰃、既に距離􏰂計測済みな􏰁で何も送信しない。 
			// さもなけれ􏰃、dstに対してsrcが持つ、他頂点とそ􏰁距離􏰁リストを送る
			if(isCyclic) Iterator.empty
			else Iterator((edge.dstId, edge.srcAttr))
		},
		// 複数􏰁srcから送られて来たリスト􏰂単純合体させる
		(m1, m2) => m1 ++ m2
	)    

    println("\n\n~~~~~~~~~ Confirm Vertices of graphWithDistance ")
    graphWithDistance.vertices.collect.foreach(println(_))

    sc.stop
  }
}
