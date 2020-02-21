package com.torres.app

import com.alibaba.fastjson.JSON
import com.torres.bean.{GmallConstants, OrderInfo}
import com.torres.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GmvApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))

    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map { case (_, value) => {
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

      val strs: Array[String] = orderInfo.create_time.split(" ")

      orderInfo.create_date = strs(0)
      orderInfo.create_hour = strs(1).split(":")(0)

      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"

      orderInfo
    }
    }
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL190826_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
