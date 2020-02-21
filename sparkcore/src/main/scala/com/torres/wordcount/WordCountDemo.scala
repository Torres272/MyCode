package com.torres.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    // 1 创建SC
     val conf: SparkConf = new SparkConf().setAppName("word_count").setMaster("local[8]")
    val sc: SparkContext = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("input")
    sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("output")
    sc.stop()

  }
}
