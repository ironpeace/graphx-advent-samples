package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Graphxsamples {

    def main(args: Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)

		// Edge情報のファイルからグラフ生成
		val graph = GraphLoader.edgeListFile(sc, "graphdata/graph_1.tsv").cache()

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
		graph.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
		graph.vertices.collect.foreach(println(_))

		// [vid, name]形式で入っているvertex情報をファイルから読み取る
		val vertexLines = sc.textFile("graphdata/vertices.csv")
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


		// taroは誰をフォローしているか
		println("\n\n~~~~~~~~~ Confirm who taro follows")
		// 各VertexからOut方向の隣接Vertexを取得
		graph2.collectNeighbors(EdgeDirection.Out)
			// リストの中から該当のものをフィルター
			.filter(v => v._1 == 1)
			// ひとつだけ選択しているのでfirstで該当行取得
			.first
			// 該当Vertexのリストを取得
			._2
			// 該当Vertexリストを標準出力
			.foreach(println(_))


		// taroは誰にフォローされているか
		println("\n\n~~~~~~~~~ Confirm who follows taro ")
		// 各VertexからIn方向の隣接Vertexを取得
		graph2.collectNeighbors(EdgeDirection.In)
			// リストの中から該当のものをフィルター
			.filter(v => v._1 == 1)
			// ひとつだけ選択しているのでfirstで該当行取得
			.first
			// 該当Vertexのリストを取得
			._2
			// 該当Vertexリストを標準出力
			.foreach(println(_))


		// taroは誰とつながっているか
		println("\n\n~~~~~~~~~ Confirm who taro connects ")
		// 各VertexからIn/Out双方向の隣接Vertexを取得
		graph2.collectNeighbors(EdgeDirection.Either)
			// リストの中から該当のものをフィルター
			.filter(v => v._1 == 1)
			// ひとつだけ選択しているのでfirstで該当行取得
			.first
			// 該当Vertexのリストを取得
			._2
			// 該当Vertexリストを標準出力
			.foreach(println(_))

		// max関数
		def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)={
			if (a._2 > b._2) a else b
		}

		// 最もフォローされているのは誰か
		println("\n\n~~~~~~~~~ Confirm who is followed by max people ")
		val maxFollowed = graph.inDegrees.reduce(max)
		println("%s is followed by %d people".format(
			users.filter(u => u._1 == maxFollowed._1).first._2, 
			maxFollowed._2
		))

		
		// 最もフォローしているのは誰か
		println("\n\n~~~~~~~~~ Confirm who follows max people ")
		val maxFollows = graph.outDegrees.reduce(max)
		println("%s follows %d people".format(
			users.filter(u => u._1 == maxFollows._1).first._2, 
			maxFollows._2
		))

		// 最もつながっているのは誰か
		println("\n\n~~~~~~~~~ Confirm who connects with max people ")
		val maxConnects = graph.degrees.reduce(max)
		println("%s connected with %d people".format(
			users.filter(u => u._1 == maxConnects._1).first._2, 
			maxConnects._2
		))

		// taroの3階層以内のつながりは誰か
		println("\n\n~~~~~~~~~ Confirm who connects with taro in depth 3 ")
		// Pregel関数を利用
		// ターゲットのVertex（この場合taro）に階層分の値(この場合3)を渡して、
		// そこからつながっているVertexに1ずつ減らして配って行って、
		// 3回繰り返した後に、0以上の値を持つVertexを抽出すれば、3階層以内のつながりが取得できる
		val depth = 3
		Pregel(
			// initialGraph
			// taroのVertexの値にdepthを、それ以外には-1を設定した状態のグラフを用意
			graph.mapVertices((vid, value) => if(vid.toInt == 1) depth else -1),
			// initialMsg
			// 最初に各Edgeを流れるMsgとして-1を設定
			-1,
			// maxIter
			// 繰り返し回数として4を設定
			depth,
            // activeDir
            // メッセージ送信方向は両方向
            EdgeDirection.Either) (
            // メッセージを受信したVerteの処理
            // 渡って来たメッセージと、自分が持っている値の大きい方を取る
            vprog = (id, attr, msg) =>  math.max(attr,msg),
            // 各Edgeをどのようにメッセージ送信するか
            sendMsg = edge => {
            	if(edge.srcAttr <= 0){
	                if(edge.dstAttr <= 0){
	                	// src側、dst側両方とも0以下の場合は何のMSGも送信しない
	                    Iterator.empty
	                }else{
	                	// src側が0以下で、dst側が1以上の場合は、src側にdstの値を-1して送る
	                    Iterator((edge.srcId, edge.dstAttr - 1))
	                }
	            }else{
	                if(edge.dstAttr <= 0){
	                	// dst側が0以下で、src側が1以上の場合は、dst側にsrcの値を-1して送る
	                    Iterator((edge.dstId, edge.srcAttr - 1))
	                }else{
	                	// src側、dst側両方とも1以上の場合はそれぞれの値を-1して反対側に送る
	                    val toDst = Iterator((edge.dstId, edge.srcAttr - 1))
	                    val toSrc = Iterator((edge.srcId, edge.dstAttr - 1))
	                    toDst ++ toSrc
	                }
	            }
	        },
	        // 同じVetexに複数値が送られて来た時、
	        // 大きい方をとる
	        mergeMsg = (a,b) => math.max(a,b)
        )
		// 3回のIterationが終わったタイミングで0以上の値を持つVertexを抽出する
        .subgraph(vpred = (vid, v) => v >= 0)
        .vertices.collect.foreach(println(_))

		// 友達の輪を探せ
		// 自分のIDをリストに入れて、次のVertexに渡すというのを繰り返す
		// Iterationが終わった時に、自分の手元にあるリストに自分のIDが入っていれば
		// 自分のIDが還流してきたと判断できる
		def findFriendCircle(maxIter:Int) = {
			Pregel(
				// initialGraph
				// 各Vertexに空のリストをセット
				graph.mapVertices((id, attr) => Set[VertexId]()), 
				// initialMsg
				// 最初に各Edgeを流れるMsgとして空のリストを設定
				Set[VertexId](), 
				// maxIter
				// 繰り返し回数として3を設定
				maxIter,
	            // activeDir
	            // メッセージ送信方向はOut
				EdgeDirection.Out) (
				// 自分のリストと、渡って来たリストを合体させる
				vprog = (id, attr, msg) => (msg ++ attr),
				// Srcが持っているリストにSrcのIDを追加してDstに渡す
				sendMsg = edge => Iterator((edge.dstId, (edge.srcAttr + edge.srcId))),
				// 複数Dstから送られて来たリストを合体させる
				mergeMsg = (a, b) => (a ++ b)		
			)
			// リストに自分のIDが入っているVertexが還流Vertex
			.subgraph(vpred = (id, attr) => attr.contains(id))
			.vertices.collect.foreach(println(_))
		}

		println("\n\n~~~~~~~~~ Confirm who belongs to friends circle in depth 3")
		findFriendCircle(3)

		println("\n\n~~~~~~~~~ Confirm who belongs to friends circle in depth 4")
		findFriendCircle(4)

		println("\n\n~~~~~~~~~ Confirm who belongs to friends circle in depth 5")
		findFriendCircle(5)

		// srcからdst宛の送金額情報を持つファイルからedgeリストを作る
		val edgelines = sc.textFile("graphdata/graph_2.csv")
		val edges: RDD[Edge[Long]]
			= edgelines.map(line => {
				val cols = line.split(",")
				Edge(cols(0).toLong, cols(1).toLong, cols(2).toLong)
			})

		// usersとedgesからGraphを作る
		val graph3 = Graph(users, edges)

		println("\n\n~~~~~~~~~ Confirm Edges Internal of graph3 ")
		graph3.edges.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph3 ")
		graph3.vertices.collect.foreach(println(_))

		println("\n\n~~~~~~~~~ Confirm triplets Internal of graph3 ")
		graph3.triplets.collect.foreach(println(_))

		// 最も大きな金額が送金されたのは誰か
		println("\n\n~~~~~~~~~ Confirm who is sended max money ")
		val whoSendedMax = graph3.mapReduceTriplets(
			// 各EdgeでどんなMsgを送信するかという実装をする
			mapFunc = edgeTriplet => {
				// dst側に、edgeの値を送る
				Iterator((edgeTriplet.dstId, edgeTriplet.attr))
			},
			// 送られたedgeの値を足して行く
			reduceFunc = (a:Long, b:Long) => a + b
		// 結果、各Vertexが受け取っている金額の合計のリストが取得できるので
		// reduce処理で最大のVertexを取得する
		).reduce((a, b) => if (a._2 > b._2) a else b)

		println("%s is sended max money(%d)".format(
			users.filter(u => u._1 == whoSendedMax._1).first._2, 
			whoSendedMax._2
		))

		// val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		// import sqlContext.createSchemaRDD

		sc.stop
    }

}