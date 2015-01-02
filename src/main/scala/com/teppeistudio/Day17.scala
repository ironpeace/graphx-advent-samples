package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD

object Day17 {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

	    val graph:Graph[Int, Int]
	    	= GraphLoader.edgeListFile(sc, "graphdata/day17.tsv").cache()

  		// println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		// graph.vertices.collect.foreach(println(_))

	    val mediation = Pregel(
	    	graph.mapVertices{ case(vid:Long, attr:Int) => Set[(VertexId, Int, Set[Set[VertexId]])]() },
	    	Set[(VertexId, Int, Set[Set[VertexId]])](),
	    	Int.MaxValue,
	    	EdgeDirection.Either
	    )(
	    	vprog = (vid, attr, msg) => {
	    		if(msg.size == 0) {
	    			attr
	    		} else {
		    		// println("vplog of [" + vid + "]~~~~~~~~~~~~ start")
		    		// println("attr ~~~~~~~~~~~~")
		    		// attr.foreach(println(_))
		    		// println("msg ~~~~~~~~~~~~")
		    		// msg.foreach(println(_))
		    		// println("vplog of [" + vid + "]~~~~~~~~~~~~ end")

		    		// 元から持っていた attr にフラグ付け
		    		val flaged_attr = for(a <- attr) yield (a._1, a._2, a._3, "a")

		    		// 受信した msg にフラグ付けし、受信した距離を1カウントアップ
	    			val flagged_and_countedUp_msg = for(m <- msg) yield (m._1, m._2 + 1, m._3, "m")

	    			// まずは、元から持っていた attr と msg を合体させる
	    			val unioned = flaged_attr ++ flagged_and_countedUp_msg

	    			// 合体させた attr のリストを、CounterId でグルーピング
	    			val grouped:Map[VertexId, Set[(VertexId, Int, Set[Set[VertexId]], String)]] = unioned.groupBy(g => g._1)

	    			// グルーピングした CounterId 毎に複数レコードあるものを1レコードにマージする
					val merged:scala.collection.immutable.Iterable[(VertexId, Int, Set[Set[VertexId]])]
					 = for((key, attrs) <- grouped) yield {
					 	// 元から１レコードしかないなら、フラグだけ除いてそのまま使う
	    				if(attrs.size == 1) {
	    					(attrs.head._1, attrs.head._2, attrs.head._3)
	    				} else {
	    					// 元から持っていたルートを取得
	    					val existed = attrs.filter(a => a._4 == "a")
	    					// ちなみにこのルートは１行、もしくは０行しかありえない
	    					if(existed.size > 1) throw new Exception("Unknown pattern")

	    					// 元から持っていたルートの距離を取得。元ルートがないなら最大値にしておく。
	    					val existedDistance = if(existed.size == 1) existed.head._2 else Int.MaxValue

	    					// 追加になったルートを取得する
	    					val added = attrs.filter(a => a._4 == "m")

	    					// その中から元ルートの距離より短い、追加ルートのものを取得する
	    					val shorters = added.filter(a => a._2 <= existedDistance)
	    					// 短いルートの値をとるか、それが存在しないなら、既存ルートの距離をとる
	    					val newDistance:Int = if(shorters.size > 0) shorters.head._2 else existedDistance

	    					// 既存ルートと短ルートを合体させる
	    					var routes = Set[Set[VertexId]]()
	    					if(existed.size == 1) routes = routes ++ existed.head._3
	    					for(s <- shorters) yield {
	    						val shorterRoutes:Set[Set[VertexId]] = s._3
	    						routes = routes ++ shorterRoutes
	    					}

	    					(attrs.head._1, newDistance, routes)
	    				}
	    			}
	    			merged.toSet
	    		}
	    	},
	    	sendMsg = edge => {
				val toSrcInfo = message(edge.dstId, edge.dstAttr, edge.srcId, edge.srcAttr)
				val toSrc = if(toSrcInfo.size == 0) Iterator.empty else Iterator((edge.srcId, toSrcInfo))

				val toDstInfo = message(edge.srcId, edge.srcAttr, edge.dstId, edge.dstAttr)
				val toDst = if(toDstInfo.size == 0) Iterator.empty else Iterator((edge.dstId, toDstInfo))

				toDst ++ toSrc
	    	},
	    	mergeMsg = (a1, a2) => {
	    		a1 ++ a2
	    	}
	    )

		val vlist = mediation.vertices.collect

  		// println("\n\n~~~~~~~~~ Confirm Vertices Internal of mediation ")
		// vlist.foreach(println(_))

		var routes = Set[(Set[VertexId], Set[Set[VertexId]])]()
		for(v <- vlist) yield {
			val route = for(c <- v._2) yield (Set(v._1, c._1), c._3)
			routes = routes ++ route.toSet
		}

  		// println("\n\n~~~~~~~~~ Confirm Routes ")
		// routes.foreach(println(_))

		val vcount = graph.vertices.count
		val bigMthr = (vcount - 1) * (vcount - 2)

		for(v <- vlist) {
			var b = 0F
			for(route <- routes) {
				var chld = 0F
				var mthr = 0F
				for(r <- route._2){
					if(r.contains(v._1)){
						chld = chld + 1
					}
				}
				if(chld > 0) {
					mthr = route._2.size
					// println("    " + v._1 + " :: chld ; " + chld + ", mthr : " + mthr)
					val d:Float = chld / mthr
					b = b + d
				}
			}
			println(" 媒介数 " + v._1 + " : " + b)
			println(" 標準化 " + v._1 + " : " + ((2 * b) / bigMthr))
		}

	    sc.stop
	}

	def message(
		myId:VertexId,
		myAttr:Set[(VertexId, Int, Set[Set[VertexId]])],
		counterId:VertexId,
		counterAttr:Set[(VertexId, Int, Set[Set[VertexId]])]
	):Set[(VertexId, Int, Set[Set[VertexId]])] = {
		if(myAttr.size == 0) {
			Set((myId, 0, Set(Set())))
		} else {
			val excluded_myId = myAttr.filter(m => m._1 != counterId)
			val excluded_counterId = excluded_myId.filter(m => !contains(counterAttr, m._1))
			val added_myId_in_route = for(e <- excluded_counterId) yield {
				val new_route_list = for(l <- e._3) yield {
					l + myId
				}
				(e._1, e._2, new_route_list.toSet)
			}
			added_myId_in_route.toSet
		}
	}

	def contains(attr:Set[(VertexId, Int, Set[Set[VertexId]])], vid:VertexId) : Boolean = {
		for(a <- attr) {
			if( a._1 == vid ) return true
		}
		return false
	}


	// def existRoutes (
	// 		attr : Array[(VertexId, Set[Set[VertexId]], Int)],
	// 		msg : Array[(VertexId, Array[Array[VertexId]], Int)]
	// 	) : Array[(VertexId, Array[Array[VertexId]], Int)] = {

	// 	val filteredMsg = msg.filter(m => existRoute(m._1, attr) != None)

	// 	for(fm <- filteredMsg) yield {
	// 		val e = existRoute(fm._1, attr).get
	// 		(fm._1, fm._2 ++ e._2, fm._3)
	// 	}
	// }

	// def newRoutes (
	// 		attr : Array[(VertexId, Array[Array[VertexId]], Int)],
	// 		msg : Array[(VertexId, Array[Array[VertexId]], Int)]
	// 	) : Array[(VertexId, Array[Array[VertexId]], Int)] = {
	// 	val nr = msg.filter(m => existRoute(m._1, attr) == None)
	// 	for(n <- nr) yield (n._1, n._2, n._3 + 1)
	// }

	// def keepRoutes (
	// 		attr : Array[(VertexId, Array[Array[VertexId]], Int)],
	// 		msg : Array[(VertexId, Array[Array[VertexId]], Int)]
	// 	) : Array[(VertexId, Array[Array[VertexId]], Int)] = {

	// 	attr.filter(a => existRoute(a._1, msg) == None)
	// }
	
	// def existRoute(vid:VertexId, routes:Array[(VertexId, Array[Array[VertexId]], Int)])
	// 	: Option[(VertexId, Array[Array[VertexId]], Int)] = {
	// 	for(r <- routes) {
	// 		if(r._1 == vid) return Some(r)
	// 	}
	// 	return None
	// }

	// def getSendInfo(
	// 		myId:VertexId, 
	// 		counterId:VertexId, 
	// 		myRoutes:Array[(VertexId, Array[Array[VertexId]], Int)],
	// 		counterRoutes:Array[(VertexId, Array[Array[VertexId]], Int)]
	// 		) : Array[(VertexId, Array[Array[VertexId]], Int)] = {

	// 	if(myRoutes.length == 0) {
	// 		// 空しか持っていなければ、counter が自分のrouteを送る
	// 		Array((myId, Array(Array()), 0))
	// 	} else {
	// 		// これから送ろうしている counter が counter じゃない route を送る
	// 		val notCounterRoutes = myRoutes.filter(r => r._1 != counterId)

	// 		// 送信先に既にあるCounterで、自分が送ったところで距離が縮まらなさそうなら送らない
	// 		val stillNotSendedRoutes = notCounterRoutes.filter(r => !isAlreadySendedAndShorterRoute(r._1, r._3, counterRoutes))

	// 		// route に 自分のIDを加える
	// 		addMyIdInRoute(myId, stillNotSendedRoutes)
	// 	}
	// }

	// def isAlreadySendedAndShorterRoute(
	// 	vid:VertexId, 
	// 	distance:Int,
	// 	counterRoutes:Array[(VertexId, Array[Array[VertexId]], Int)]
	// 	) : Boolean = {
	// 	for(c <- counterRoutes) {
	// 		if(c._1 == vid && c._3 > distance + 1 ) return true
	// 	}
	// 	return false
	// }

	// def addMyIdInRoute(myId:VertexId, routes:Array[(VertexId, Array[Array[VertexId]], Int)])
	// 	: Array[(VertexId, Array[Array[VertexId]], Int)] = {
	// 	// println("~~~~~ addMyIdInRoute")
	// 	for(r <- routes) yield {
	// 		// println(" --- ")
	// 		val newRoutes = for(route <- r._2) yield (route :+ myId).sorted
	// 		// newRoutes.foreach(n => println(n.mkString(",")))
	// 		(r._1, newRoutes, r._3)
	// 	}
	// }

}