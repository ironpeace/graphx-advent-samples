package com.teppeistudio

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object FindCircleGraphSample {

    def main(args : Array[String]) = {

		val conf = new SparkConf()
		val sc = new SparkContext("local", "test", conf)
		val graph = GraphLoader.edgeListFile(sc, "graphdata/find_circle_graph_sample.tsv").cache()

		sc.stop
    }

}