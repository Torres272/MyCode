package com.torres.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateWordCount01 {

  var updateFun: (Seq[Int], Option[Int]) => Option[Int] = (x,y)=>{
    //计算当前批次的和
    val sum: Int = x.sum
    //求出上次的总和
    val lastSum: Int = y.getOrElse(0)
    //返回Some---option有两种None和Some
    Some(lastSum+ sum)

  }

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RDDWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //checkPoint
    ssc.checkpoint("input")

    //3.创建DStream，从端口读取
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //4.转换为元组
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1))

    //5.使用有状态进行类加并打印
    wordToOneDStream.updateStateByKey(updateFun).print()


    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
