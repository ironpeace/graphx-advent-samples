package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object Convert2TreeSample {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)
		val graph = GraphLoader.edgeListFile(sc, "graphdata/graph_1.tsv").cache()

		println("~~~~~~~~~~ Confirm vertices")
		graph.vertices.collect.foreach(println(_))

		println("~~~~~~~~~~ Confirm edges")
		graph.edges.collect.foreach(println(_))

		// val sampleMap = Map( "name" -> "name",
		// 	"children" -> List(	Map("name" -> "child1",
		// 							"children" -> List(	Map("name" -> "child1-1","size" -> 10),
		// 												Map("name" -> "child1-2","size" -> 20),
		// 												Map("name" -> "child1-3","size" -> 30)))))
		// println("~~~~~~~~~~ Confirm sample map")
		// println(sampleMap)

		val neighbors = graph.collectNeighbors(EdgeDirection.Out).cache()

		// println("~~~~~~~~~~ Confirm neighbors")
		// neighbors.collect.foreach(println(_))

		// List((vid, List(vid))という形のデータに整形する
		val neighborsList = neighbors.map(x => (x._1.toLong, x._2.map(v => v._1.toLong))).collect

		// mapに格納済みのvidを格納するリスト
		var mappedList = Set[Long]()

		// rootで指定されたvertex配下のvertexリストをネストした階層に再帰的に格納していく
		def list2Map(root:Long, list:Array[(Long, Array[Long])]):Map[String, Any] = {
			// 渡されたリストからrootの行を取得
			val rootNode = list.filter(x => x._1 == root)(0)

			//println(mappedList)

			if(rootNode._2.length > 0 && !mappedList.contains(root)){
				// root配下にvertexがあれば、そのリストを再帰的にMap化する
				// 且つ、既にMap化されていないことが条件
				// 既にMap化されているものを再帰処理に回してしまうと、1->2->1のような場合に、無限ループしてしまう

				// 該当rootをmap化リストに格納
				mappedList = mappedList ++ Set(root)

				// 該当root配下のvertexリストをMap化したものを取得
				val children:Array[Map[String, Any]] = for(n <- rootNode._2) yield list2Map(n, list)

				// Map化
				Map("name" -> rootNode._1, "size" -> rootNode._2.length, "children" -> children)
			}else{
				// 該当root配下にvertexリストが無いなら該当rootだけをMap化する

				// 該当rootをmap化リストに格納
				mappedList = mappedList ++ Set(root)
				
				// Map化
				Map("name" -> rootNode._1, "size" -> rootNode._2.length)
			}
		}

		val neighborsMap = list2Map(1, neighborsList)

		def printNm(layer:Int, nm:Map[String, Any]):Unit = { 
			nm.foreach{ 
				case ("name", name) => for(l <- 0 to layer) print("***"); println("name : " + name);
				case ("size", size) => for(l <- 0 to layer) print("***"); println("size : " + size);
				case ("children", children:Array[Map[String, Any]]) => children.foreach(n => printNm(layer + 1, n))
				case _ => throw new Exception("unknown data")
			}
		}

		printNm(0, neighborsMap)



		sc.stop
    }

}