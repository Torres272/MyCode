package com.torres.transform

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object ReduceByKeyAndWindowWordCount {
  def main(args: Array[String]): Unit = {
    /*
（1）window(windowLength, slideInterval): 基于对源DStream窗化的批次进行计算返回一个新的Dstream；
（2）countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数；
（3）reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流；
（4）reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]): 当在一个(K,V)对的DStream上调用此函数，会返回一个新(K,V)对的DStream，此处通过对滑动窗口中批次数据使用reduce函数来整合每个key的value值。
（5）reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 这个函数是上述函数的变化版本，每个窗口的reduce值都是通过用前一个窗的reduce值来递增计算。通过reduce进入到滑动窗口数据并”反向reduce”离开窗口的旧数据来实现这个操作。
 */

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RDDWordCount").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("input")

    //3.读取端口数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.WordCount
    lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKeyAndWindow((x:Int,y:Int)=>x+y,(a:Int,b:Int)=>a-b,Seconds(6),Seconds(3),new HashPartitioner(2 ),(x:(String,Int))=>x._2>0).print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
