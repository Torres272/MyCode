package com.torres.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketWordCount")

    //2.创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //打印当前线程名->只运行一次(Driver)
    println(s"111${Thread.currentThread().getName}")

    //3.读取端口数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.将DStream转换为RDD操作，计算WordCount
    val wordToCountDStream: DStream[(String, Int)] = lineDStream.transform(rdd => {

      //打印当前线程名
      println(s"222${Thread.currentThread().getName}")

      val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
      val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(x => {

        //打印当前线程名->看数据量，每一个元素打印一次
        println(s"333${Thread.currentThread().getName}")
        (x, 1)
      })
      val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
      wordToCountRDD
    })
    //5.打印数据
    wordToCountDStream.print()

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
