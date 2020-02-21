package com.torres.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf( ).setMaster("local[*]").setAppName("StreamWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(5 ))

    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    lineStream.transform(x=>{
      println(s"222${Thread.currentThread().getName}${x.toString()}")
      x.flatMap(_.split(" ")).map(x=>{
        println(s"333${Thread.currentThread().getName}")
        (x,1)
      })
    }).reduceByKey(_+_).print()


    ssc.start()

    ssc.awaitTermination()



  }
}
