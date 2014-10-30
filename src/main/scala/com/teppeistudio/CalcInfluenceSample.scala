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
		val vs: RDD[(VertexId, Long)]	// Longは自分が持っている愛のストック（loveStock）
			= vertexLines.map(line => {
				val cols = line.split(",")
				(cols(0).toLong, cols(1).toLong)
			})

		val edgeLines = sc.textFile("graphdata/calc_influence/1/edges.csv")
		val es: RDD[Edge[Long]]	// Longは、dstに配る愛の量（deliverLove）
			= edgeLines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
			})

		// 愛し合う関係をグラフ構造にする
		val graph1:Graph[Long, Long] = Graph(vs, es)
		println("~~~~~~~~~~ 愛のグラフ")
		graph1.vertices.collect.foreach(println(_))
		graph1.edges.collect.foreach(println(_))

		// 突如、特定の人物（Vertex）の愛を奪いさる者「搾取者」が現れる
		val exploiter:RDD[(VertexId, Long)] = sc.parallelize(Array((0L, 0L)))
		val vs2 = vs.union(exploiter)

		// 「搾取者」はID:1の者から、そのどん欲さを持って全ての愛を奪い去って行く
		// val exploited: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 0L, Long.MaxValue)))
		val exploited: RDD[Edge[Long]] = sc.parallelize(Array(Edge(1L, 0L, 1000000L)))
		val es2 = es.union(exploited)

		val graph2:Graph[Long, Long] = Graph(vs2, es2)
		println("~~~~~~~~~~ 搾取者の参入")
		graph2.vertices.collect.foreach(println(_))
		graph2.edges.collect.foreach(println(_))

		// 愛のグラフにどんな変化が巻き起こるか
		// 1が搾取されたことによって、愛を失ってしまうものは誰か
		// 全てでなくても、みんなどの程度の愛を失ってしまうのか
		// それは１の愛に皆がどれだけ影響を受けていたかを示す

		println("~~~~~~~~~~~~~~~~~~~~ nonExploitedGraph")
		val nonExploitedGraph = calcInfluence(graph1)

		println("~~~~~~~~~~~~~~~~~~~~ exploitedGraph")
		val exploitedGraph = calcInfluence(graph2)

		val nonExploitedVertex:RDD[(VertexId, Long)]
			 = nonExploitedGraph.vertices.map(v => (v._1, v._2._1))

		val verifyGraph:Graph[(Long, Long), Float]
			= exploitedGraph.outerJoinVertices(nonExploitedVertex){
				(vid:VertexId, exploitedAttr:(Long, Long, Long, Long), nonExploitedResult:Option[Long]) => {
					(nonExploitedResult.getOrElse(0L), exploitedAttr._1)
				}
			}

		println("~~~~~~~~~~ 比較")
		verifyGraph.vertices.collect.foreach{ v:(VertexId, (Long, Long)) => 
			println(v._1 + " : " + v._2._1 + " -> " + v._2._2)
		}

		sc.stop

    }

	// 課題
	// 1と、同じConnectedConponentにいない人は、本来影響を全くうけない人のはずなのに、影響をうけているように見えてしまう
	// 1とのout側に絶対に出てこない人は...（同上）

    def calcInfluence(graph:Graph[Long, Long]):Graph[(Long, Long, Long, Long), Float] = {

		// 各々が隣人に愛の総量を算出
		val sumAttrList:VertexRDD[Long] = graph.mapReduceTriplets(
				mapFunc = edge => Iterator((edge.srcId, edge.attr)),
				reduceFunc = (a:Long, b:Long) => a + b
			)

		println("~~~~~~~~~~ 隣人への愛の総量")
		sumAttrList.collect.foreach(println(_))

		// 隣人への愛の総量の値を、各々に持たせる
		// VD:(愛のストック、隣人へ配る愛の準備、隣人への愛の総量、当初の愛のストック), ED:(配っている愛の量)
		val graph3:Graph[(Long, Long, Long, Long), Long]  
			= graph.outerJoinVertices(sumAttrList){ 
				(vid:VertexId, oldAttr:Long, sumEdgesAttrOpt:Option[Long]) => {
					(oldAttr, 0, sumEdgesAttrOpt.getOrElse(0), oldAttr)
				}
			}

		println("~~~~~~~~~~ 隣人への愛の総量をVertexに持ったグラフ")
		graph3.vertices.collect.foreach(println(_))
		graph3.edges.collect.foreach(println(_))

		// Edgeの値を、dstへの愛の量ではなく、srcのが配っている愛の総量に対する割合にする
		// VD:(愛のストック、隣人へ配る愛の準備、隣人への愛の総量、当初の愛のストック), ED:(配っている愛の割合)
		val graph4:Graph[(Long, Long, Long, Long), Float]  
			= graph3.mapTriplets(edge => edge.attr.toFloat / edge.srcAttr._3.toFloat)

		// さあ愛の精算のはじまりだ！
		val graph5:Graph[(Long, Long, Long, Long), Float] = Pregel(
				graph4, 
				0L, 
				activeDirection = EdgeDirection.Out) (

			// dstから愛（msg）を受け取ったらどうするか
			(id, attr, msg) => {
				val stock = attr._1	// 愛のストック
				val ready = attr._2	// 愛の準備
				val sum = attr._3 // 配らなければならない愛の総量
				val origin = attr._4 // 清算前の愛のストック

				// dstから愛を受け取ることで一時的に増加する愛のストック
				val tmpStock = stock + msg

				// その愛は、配らなければならない愛の量に足りているか？
				val isEnough = if(tmpStock >= sum) true else false

				// 愛を配った後、残る愛の量は？
				// 受け取った愛とストックの愛が、配らなければならない愛の量より大きいのなら、
				// 配りきれなかった愛が残る
				// さもなければ、全てくばりきるので、残り愛の量は0.
				//val afterSum = Math.max(sum - tmpStock, 0)
				val afterSum = if(isEnough) tmpStock - sum else 0

				// これから配る愛の量は、
				// 配らなければならない愛の量が、配れる愛の量を超えているのなら配れる愛の量、
				// さもなければ、配れる量
				//val afterReady = Math.min(tmpStock, sum)
				val afterReady = if(isEnough) sum else tmpStock

				// 残る愛のストックは、愛を配った後のストック
				// val afterStock = Math.max(tmpStock - sum, 0)
				val afterStock = if(isEnough) tmpStock - sum else 0

				(afterStock, afterReady, afterSum, origin)
			},

			// それぞれの愛の関係において、どれだけの量の愛を配るか
			edge => {
				val stock = edge.srcAttr._1	// 愛のストック
				val ready = edge.srcAttr._2	// 愛の準備
				val sum = edge.srcAttr._3 // 配らなければならない愛の総量
				val origin = edge.srcAttr._4 // 清算前の愛のストック
				var rate = edge.attr // 愛の配分割合

				if(sum <= 0){
					// 愛を配りきっているのであれば、何も配らない
					Iterator.empty
				}else{
					// 配るべき愛があるのであれば、edge毎の割合で按分した愛を配る
					val msg = stock * rate
					Iterator((edge.dstId, msg.toLong))
				}
			},
			// 複数Srcから遅れてきた愛は余すこと無く受け止める
			(a, b) => (a + b)
		)

		println("~~~~~~~~~~ 精算結果")
		graph5.vertices.collect.foreach(println(_))
		graph5.edges.collect.foreach(println(_))

		return graph5
    }

}