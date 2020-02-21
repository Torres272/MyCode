package com.torres.transform

import com.torres.transform.UpdateStateWordCount01.updateFun
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateWordCount02 {

  val getSSC: () => StreamingContext = ()=>{
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

    ssc
  }



  def main(args: Array[String]): Unit = {
    //getActiveOrCreate  从input进行读取
    //wordcount1不能断点续传，wordcount2可以
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("input",getSSC)

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
