package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Day14 {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    val graph:Graph[Int, Int]
    	= GraphLoader.edgeListFile(sc, "graphdata/day14.tsv").cache()

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))

	val circleGraph = Pregel(
		// 各頂点に空􏰁リストをセットしたグラフを処理する
		graph.mapVertices((id, attr) => Set[VertexId]()),
		// 最初に辺を流すメッセージ：空グラフ
		Set[VertexId](),
		// 4階層以内で還流しているところを探す
		4,
		// メッセージを送る方向
		EdgeDirection.Out) (
			// 自分􏰁リストと、渡って来たリストを合体させる
			(id, attr, msg) => (msg ++ attr),
			// Srcが持っているリストにSrc􏰁IDを追加してDstに渡す
			edge => Iterator((edge.dstId, (edge.srcAttr + edge.srcId))),
			// 複数Srcから送られて来たリストを合体させる
			(a, b) => (a ++ b)
		// リストに自分􏰁IDが入っている頂点が「輪」􏰁中にいる頂点
		).subgraph(vpred = (id, attr) => attr.contains(id))

    println("\n\n~~~~~~~~~ Confirm Vertices of circleGraph ")
    circleGraph.vertices.collect.foreach(println(_))

    sc.stop
  }
}
