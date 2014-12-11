package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Day11 {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext("local", "test", conf)

    val graph = GraphLoader.edgeListFile(sc, "graphdata/day11.tsv").cache()

    println("\n\n~~~~~~~~~ Confirm Vertices Internal of graph ")
    graph.vertices.collect.foreach(println(_))

    println("\n\n~~~~~~~~~ Confirm Edges Internal of graph ")
    graph.edges.collect.foreach(println(_))

    val cc:Graph[Long, Int] = graph.connectedComponents

    println("\n\n~~~~~~~~~ Confirm Vertices Connected Components ")
    cc.vertices.collect.foreach(println(_))

    val cc_label_of_vid_2:Long = cc.vertices.filter{case (id, label) => id == 2}.first._2

    println("\n\n~~~~~~~~~ Confirm Connected Components Label of Vertex id 2")
    println(cc_label_of_vid_2)

    val vertices_connected_with_vid_2:RDD[(Long, Long)]
      = cc.vertices.filter{case (id, label) => label == cc_label_of_vid_2}

    println("\n\n~~~~~~~~~ Confirm vertices_connected_with_vid_2")
    vertices_connected_with_vid_2.collect.foreach(println(_))

    val vids_connected_with_vid_2:RDD[Long] = vertices_connected_with_vid_2.map(v => v._1)
    println("\n\n~~~~~~~~~ Confirm vids_connected_with_vid_2")
    vids_connected_with_vid_2.collect.foreach(println(_))

    val vids_list:Array[Long] = vids_connected_with_vid_2.collect
    val graph_include_vid_2
      = graph.subgraph(vpred = (vid, attr) => vids_list.contains(vid))

    println("\n\n~~~~~~~~~ Confirm graph_include_vid_2 ")
    graph_include_vid_2.vertices.collect.foreach(println(_))

    sc.stop
  }

}