package com.torres.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.torres.bean.{CouponAlertInfo, EventLog, GmallConstants}
import com.torres.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlterApp {
  def main(args: Array[String]): Unit = {

    //创建sparkConf，创建StreamingContext，读取Kafka数据
    val sparkConf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.KAFKA_TOPIC_EVENT))

    //创建时间转换类，将读过来的数据转换为样例类
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map { case (_, value) =>
      val log: EventLog = JSON.parseObject(value, classOf[EventLog])

      //补充日期及小时字段
      val ts: Long = log.ts
      val logTs: String = sdf.format(new Date(ts))
      log.logDate = logTs.split(" ")(0)
      log.logHour = logTs.split(" ")(1)
      log
    }


    //需求：同一设备，30秒内三次以上用不同账号登录并领取优惠券，并且没有浏览商品
    //开窗
    val windowEventLog: DStream[EventLog] = eventLogDStream.window(Seconds(30))

    //分组
    val groupByMidDStream: DStream[(String, Iterable[EventLog])] = windowEventLog.map(log => (log.mid, log)).groupByKey()


    //过滤"clickItem", 40), new RanOpt("coupon"
    val IsAlterInfoDStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map { case (mid, list) =>
      //用户名
      val uids = new java.util.HashSet[String]()
      //领取优惠券的商品
      val itemIds = new java.util.HashSet[String]()
      //用户的行为信息
      val events = new java.util.ArrayList[String]()
      var flag = true

      breakable {
        list.foreach(item => {
          events.add(item.evid)
          if ("clickItem".equals(item.evid)) {
           // println(item.evid)
            flag = false
            break();

          } else if ("coupon".equals(item.evid)) {
            //println(item.evid)
            uids.add(item.uid)
            itemIds.add(item.itemid)
          }
        })
      }
      (uids.size() >= 3 && flag, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    IsAlterInfoDStream.filter(_._1).map(_._2).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
