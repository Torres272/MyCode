package com.torres.app


import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.torres.bean.{GmallConstants, StartUpLog}
import com.torres.handler.DauHandler
import com.torres.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {

    //1.创建spark conf 及 ssc
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    //val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.创建从kafka读取数据的DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.KAFKA_TOPIC_STARTUP))

    //3.格式化字符串
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //4.将json转换为样例类
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map { case (_, value) =>
      val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = log.ts
      val logDateHour: String = sdf.format(new Date(ts))
      val logDateHourArr: Array[String] = logDateHour.split(" ")
      log.logDate = logDateHourArr(0)
      log.logHour = logDateHourArr(1)
      log
    }

    //5.跨区间去重
    val filterDataByRedisDStream: DStream[StartUpLog] = DauHandler.filterDataByRedis(ssc,startLogDStream)
    filterDataByRedisDStream.cache()

    filterDataByRedisDStream.count().print()

    //6.同批次去重
    val filterDataByBatchDStream: DStream[StartUpLog] = DauHandler.filterDataByBatch(filterDataByRedisDStream)

    filterDataByBatchDStream.cache()

    filterDataByBatchDStream.count().print()

    //7.保存到redis
    DauHandler.saveDataToRedis(filterDataByBatchDStream)

    //8.写入到HBase
    filterDataByBatchDStream.foreachRDD(rdd =>
      rdd.saveToPhoenix("GMALL190826_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "LOGTYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
    )

    //启动sparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }
}
