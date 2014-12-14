package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Day13 {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    val graph:Graph[Int, Int]
    	= GraphLoader.edgeListFile(sc, "graphdata/day13.tsv").cache()

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))

	def sendMsgFunc(edge:EdgeTriplet[Int, Int]) = {
		if(edge.srcAttr <= 0){
			if(edge.dstAttr <= 0){
				// 両方とも0以下なら何も送信しない
				Iterator.empty
			}else{
				// 片方0以下で、片方0より大きいなら大きい方から1減算して送信する
				Iterator((edge.srcId, edge.dstAttr - 1))
			}
		}else{
			if(edge.dstAttr <= 0){
				// 片方0以下で、片方0より大きいなら大きい方から1減算して送信する
				Iterator((edge.dstId, edge.srcAttr - 1))
			}else{
				// 両方とも0より大きいなら、双方􏰁値を1減算して、双方に送信する
				val toSrc = Iterator((edge.srcId, edge.dstAttr - 1))
				val toDst = Iterator((edge.dstId, edge.srcAttr - 1))
				toDst ++ toSrc
			}
		}
	}

	val friends = Pregel(
		graph.mapVertices((vid, value)=> if(vid == 1) 2 else -1),

		// 最初􏰁イテレーションで送信するメッセージ
		-1,

		// 指定階層値􏰁回数イテレーションする
		2,

		// 双方向にメッセージ送信する
		EdgeDirection.Either
	)(
		// 受信した値と自分􏰁値􏰁うち、大きい方を設定する
		vprog = (vid, attr, msg) => math.max(attr, msg),

		// イテレーション毎に辺上に何を流すか決定する処理(後述)
		sendMsgFunc,

		// 複数􏰁Edgeから値を受信したら大きい方をとる
		(a, b) => math.max(a, b)
	)
	// 頂点􏰁値が0以上􏰁頂点を抽出
	.subgraph(vpred = (vid, v) => v >= 0)

    println("\n\n~~~~~~~~~ Confirm Vertices of friends ")
    friends.vertices.collect.foreach(println(_))

    sc.stop
  }

}