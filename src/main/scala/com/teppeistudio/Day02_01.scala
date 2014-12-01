package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Day02_01 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		// Edge情報のファイルからグラフ生成
		val graph = GraphLoader.edgeListFile(sc, "graphdata/day02-01-edges.tsv").cache()

		// [vid, name]形式で入っているvertex情報をファイルから読み取る
		val vertexLines = sc.textFile("graphdata/day02-01-vertices.csv")
		val users: RDD[(VertexId, String)]
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, cols(1))
			})

		// vertex情報をgraphにJoinさせてgraph2を生成する
		val graph2 = graph
						// デフォルトではVertexのAttributeとして
						// intの1が入っているので、それをStringの空文字にする
						.mapVertices((id, attr) => "")
						// user情報をJoinして、VertexのAttributeとしてユーザ名を入れる
						.joinVertices(users){(vid, empty, user) => user}

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph2 ")
		graph2.vertices.collect.foreach(println(_))

		// vertex情報をgraphにJoinさせてgraph3を生成する
		val graph3 = graph
						// デフォルトではVertexのAttributeとして
						// intの1が入っているので、それをStringの空文字にする
						.mapVertices((id, attr) => "")
						// user情報をJoinして、VertexのAttributeとしてユーザ名を入れる
						.outerJoinVertices(users){(vid, empty, user) => user.getOrElse("None")}

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph3 ")
		graph3.vertices.collect.foreach(println(_))

		sc.stop
	}


}