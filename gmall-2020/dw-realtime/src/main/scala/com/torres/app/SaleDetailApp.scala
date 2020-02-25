package com.torres.app

import java.util

import com.alibaba.fastjson.JSON
import com.torres.bean.{GmallConstants, OrderDetail, OrderInfo, SaleDetail}
import com.torres.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.消费order_info,order_detail,user_info
    val orderInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO_TOPIC))
    val orderDetailDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
    val userInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_USER_INFO_TOPIC))

    //4.转换为样例类对象
    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoDStream.map { case (_, value) =>
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
      //处理创建日期及小时 2020-02-21 12:12:12
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      //手机号脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4)._1 + "*******"
      (orderInfo.id, orderInfo)
    }

    val idToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.map { case (_, value) =>
      val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    }

    //5.join
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(idToOrderDetailDStream)

    //6.对join后的数据进行处理
    joinDStream.mapPartitions(iter => {
      //定义连接，及返回值列表
      val jedisClient: Jedis = RedisUtil.getJedisClient
      var list = new ListBuffer[SaleDetail]()
      implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

      //处理iter中的数据
      iter.foreach { case (id, (orderInfoOpt, orderDetailOpt)) =>
        val orderInfoRedisKey = s"orderInfo:$id"
        val orderDetailRedisKey = s"orderDetail:$id"

        //判断orderInfo是否定义
        if (orderInfoOpt.isDefined) {
          //orderDetail不空
          val orderInfo: OrderInfo = orderInfoOpt.get
          if (orderDetailOpt.isDefined) {
            val orderDetail: OrderDetail = orderDetailOpt.get
            list += new SaleDetail(orderInfo, orderDetail)
          }

          //将orderInfo加入Redis
          val orderJson: String = Serialization.write(orderInfo)
          jedisClient.setex(orderInfoRedisKey, 300, orderJson)

          //查询orderDetail,并加入到list
          val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailRedisKey)
          import scala.collection.JavaConversions._
          orderDetailSet.foreach(item => {
            val detail: OrderDetail = JSON.parseObject(item, classOf[OrderDetail])
            list += new SaleDetail(orderInfo, detail)
          })

        } else {
          val orderDetail: OrderDetail = orderDetailOpt.get

          if (jedisClient.exists(orderInfoRedisKey)) {
            val orderInfoJson: String = jedisClient.get(orderInfoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            list += new SaleDetail(orderInfo, orderDetail)
          } else {
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailRedisKey, orderDetailJson)
            jedisClient.expire(orderDetailRedisKey, 300)
          }
        }
      }

      jedisClient.close()
      list.toIterator
    }).print()


    //启动
    ssc.start()
    ssc.awaitTermination()


  }
}
