package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

object Day16 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

	    val graph:Graph[Int, Int]
	    	= GraphLoader.edgeListFile(sc, "graphdata/day16_1.tsv").cache()

	    // 綺麗にクラスタリングされたパターン
	    val graph1 = graph.mapVertices{ case(vid:Long, attr:Int) => if(vid < 4) 1 else 2 }

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph1 ")
		graph1.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph1))

	    // 変なクラスタリングされたパターン
	    val graph2 = graph.mapVertices{ case(vid:Long, attr:Int) => if(vid < 2) 1 else 2 }

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph2 ")
		graph2.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph2))

	    // クラスタがひとつだけになってしまっているパターン
	    val graph3 = graph.mapVertices{ case(vid:Long, attr:Int) => 1 }

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph3 ")
		graph3.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph3))

	    // ひとつの頂点にひとつのクラスタが割り当たっているパターン
	    val graph4 = graph.mapVertices{ case(vid:Long, attr:Int) => vid.toInt }

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph4 ")
		graph4.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph4))

	    // ３つにクラスタされたパターン
	    val graph5 = graph.mapVertices{ case(vid:Long, attr:Int) => 
	    	if(vid < 3) 1 
	    	else if(vid < 5) 2
	    	else 3 
	    }
		graph5.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph5))

	    val graph6:Graph[Int, Int]
	    	= GraphLoader.edgeListFile(sc, "graphdata/day16_2.tsv").cache()

	    // 綺麗にクラスタリングされたパターン
	    val graph7 = graph6.mapVertices{ 
	    	case(vid:Long, attr:Int) => {
	    		if(0 < vid && vid < 4) 1 
	    		else if(3 < vid && vid < 7) 2
	    		else 3
	    	}
	    }

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph7 ")
		graph7.vertices.collect.foreach(println(_))
		println("modurality : " + modurality(graph7))

		sc.stop
	}

	def modurality(graph:Graph[Int, Int]):Float = {

		// クラスタ内のEdge数を取得（双方向で計上するので２倍しておく）
		def edgesCntOfCluster(graph:Graph[Int, Int], cluster:Int):Float = {
			graph.subgraph(vpred = (vid, attr) => attr == cluster).edges.count.toFloat * 2
		}

		// クラスタ間にまたがるEdge数を取得（これは重複カウントしない）
		def edgesCntBetweenClusters(graph:Graph[Int, Int], clusterAandB:Set[Int]):Float = {
			if(clusterAandB.size != 2) throw new Exception("Args Set of clusterAandB must be size 2")
			graph.subgraph(epred = { edge => 
				if(edge.srcAttr == clusterAandB.head && edge.dstAttr == clusterAandB.tail.head) true
				else if(edge.srcAttr == clusterAandB.tail.head && edge.dstAttr == clusterAandB.head) true
				else false
			}).edges.count.toFloat
		}

		def clusterPairsContains(clusterPairs:Set[Set[Int]], cluster:Int):Set[Set[Int]] = {
			clusterPairs.filter(cp => cp.contains(cluster))
		}

		// グラフ内Edge総数（双方向で計上するので２倍しておく）
		val edgesCnt:Float = graph.edges.count.toFloat * 2
		// println("--- edgesCnt : " + edgesCnt)

		// クラスタIDのリストを取得する
		val clusters:Array[Int] = graph.vertices.map(v => (v._2,1)).groupBy(g => g._1).map(g => g._1).collect

		// println("--- clusters")
		clusters.foreach(println(_))

		var clusterPairs = Set[Set[Int]]()
		for( c1 <- clusters ) yield {
			for( c2 <- clusters ) {
				if(c1 != c2) clusterPairs = clusterPairs + Set(c1, c2)
			}
		}
		// println("--- clusterPairs.size : " + clusterPairs.size)
		// println("--- clusterPairs")
		clusterPairs.foreach(println(_))

		var mod:Float = 0.0F
		for( c <- clusters ){
			val ecoc = edgesCntOfCluster(graph, c)
			// println("------ edgesCntOfCluster : " + ecoc)

			var aii = ecoc / edgesCnt

			val cpc = clusterPairsContains(clusterPairs, c)

			for(cp <- cpc) {
				val ecbc = edgesCntBetweenClusters(graph, cp)
				aii = aii + (ecbc / edgesCnt)
			}

			mod = mod + (( ecoc / edgesCnt ) - ( aii * aii))
		}

		// println("--- mod : " + mod )
		return mod
	}
}

