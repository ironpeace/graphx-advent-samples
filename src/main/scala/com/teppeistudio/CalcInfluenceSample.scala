package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CalcInfluenceSample {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		val vertexLines = sc.textFile("graphdata/calc_influence/1/vertices.csv")
		val vs: RDD[(VertexId, Long)]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, cols(1).toLong)
			})

		val edgeLines = sc.textFile("graphdata/calc_influence/1/edges.csv")
		val es: RDD[Edge[Long]]
			= edgeLines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
			})

		val graph1:Graph[Long, Long] = Graph(vs, es)
		println("graph1")
		graph1.vertices.collect.foreach(println(_))
		graph1.edges.collect.foreach(println(_))

		val exploiter:RDD[(VertexId, Long)] = sc.parallelize(Array((0L, 0L)))
		val vs2 = vs.union(exploiter)

		val exploited: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 0L, Long.MaxValue)))
		val es2 = es.union(exploited)

		val graph2:Graph[Long, Long] = Graph(vs2, es2)
		println("graph2")
		graph2.vertices.collect.foreach(println(_))
		graph2.edges.collect.foreach(println(_))

		// 各Vertexからoutしているedgeのattrの、Vertex毎の合計を取得
		val sumAttrList:VertexRDD[Long] = graph2.mapReduceTriplets(
				mapFunc = edge => Iterator((edge.srcId, edge.attr)),
				reduceFunc = (a:Long, b:Long) => a + b
			)

		println("sumAttrList")
		sumAttrList.collect.foreach(println(_))

		// 各Vertex毎のEdgeのattrの合計を、VertexにJOINして新しいGraphを作る
		// このGraphのVertexのattrには、Vertex自身の値と、Vertexからoutするedgeのattrの値の合計値をtapleで持つ
		val graph3:Graph[(Long, Long), Long] 
			= graph2.outerJoinVertices(sumAttrList){ 
				(vid:VertexId, oldAttr:Long, sumEdgesAttrOpt:Option[Long]) => {
					(oldAttr, sumEdgesAttrOpt.getOrElse(0))
				}
			}

		println("graph3")
		graph3.vertices.collect.foreach(println(_))
		graph3.edges.collect.foreach(println(_))


		// TODO: Iterationの度にEdge.attrを減産していかないといけないが、やり方が分からない...
		val graph4 = Pregel(
				graph3, 
				0L, 
				activeDirection = EdgeDirection.Out) (

			// 送信されたMSG値は自分の値に足し込む
			(id, attr, msg) => (attr._1 + msg, attr._2),

			// 各dstに対して、自分が持っている値を、自分から出ている各Edgeのattrの比率で按分して送信する
			edge => {
				val myVal = edge.srcAttr._1
				val sumVal = edge.srcAttr._2

				// 対象Edgeの値が0 もしくは、自分が持っている値が各Edgeのattr合計に満たない場合は何も送信しない
				if(edge.attr == 0L || myVal < sumVal ){
					Iterator.empty
				}else{
					val msgVal = edge.attr / sumVal * myVal
					Iterator((edge.dstId, msgVal))
				}

			},
			// 複数Srcから遅れてきたMSGは全て足し込む
			(a, b) => (a + b)
		)
		
		graph4.vertices.collect.foreach(println(_))





		sc.stop

    }

}