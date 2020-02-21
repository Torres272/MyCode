package com.torres.handler

import java.text.SimpleDateFormat

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdToCountHandler {


  //时间格式化对象
  private val sdf = new SimpleDateFormat("HH:mm")

  private val redisKey = "last_hour_ad_click"


  def saveLastHourAdToCount(adsLogDStream: DStream[Ads_log]): Unit = {
    //先进行开窗
    adsLogDStream
      .window(Minutes(2))
      .map(adsLog => (adsLog.adid, 1L))
      .reduceByKey(_ + _)
      .map { case (key, value) => (key, value.toString) }
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          val jedisClient: Jedis = RedisUtil.getJedisClient

          val list: List[(String, String)] = iter.toList
          import scala.collection.JavaConversions._

          if (list.nonEmpty) {
            jedisClient.hmset(redisKey, list.toMap)
          }
          //          if(jedisClient.hgetAll(redisKey).size()>=0){
          //            println("--------------开始--------------------")
          //            jedisClient.hgetAll(redisKey).foreach(println)
          //            println("--------------结束--------------------")
          //          }
          jedisClient.close()
        })
      })
  }

  def saveLastHourAdToCount01(adsLogDStream: DStream[Ads_log]): Unit = {
    adsLogDStream
      .window(Minutes(2))
      .map(adsLog => ((adsLog.adid, sdf.format(adsLog.timestamp)), 1L))
      .reduceByKey(_ + _)
      .map { case ((ad, hm), count) => (ad, (hm, count)) }
      .groupByKey()
      .mapValues(iter => {
        val list: List[(String, Long)] = iter.toList.sortWith(_._1 < _._1)
        import org.json4s.JsonDSL._
        JsonMethods.compact(list)
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          val jedisClient: Jedis = RedisUtil.getJedisClient
          val list: List[(String, String)] = iter.toList
          import scala.collection.JavaConversions._
          if (list.nonEmpty) {
            jedisClient.hmset(redisKey, list.toMap)
          }

          if (jedisClient.hgetAll(redisKey).size() >= 0) {
            println("--------------开始--------------------")
            jedisClient.hgetAll(redisKey).foreach(println)
            println("--------------结束--------------------")
          }
          jedisClient.close()
        })
      })
  }

  def saveLastHourAdToCount02(adsLogDStream: DStream[Ads_log]): Unit = {
    adsLogDStream
      .window(Minutes(2))
      .map(adsLog => ((adsLog.adid, sdf.format(adsLog.timestamp)), 1L))
      .reduceByKey(_ + _)
      .map { case ((ad, hm), count) => (ad, (hm, count)) }
      .groupByKey()
      .mapValues(iter => {
        val list: List[(String, Long)] = iter.toList.sortWith(_._1 < _._1)
        import org.json4s.JsonDSL._
        JsonMethods.compact(list)
      })
      .foreachRDD(rdd => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //          val list: List[(String, String)] = rdd.cole
        val array: Array[(String, String)] = rdd.collect()
        val list: List[(String, String)] = array.toList
        import scala.collection.JavaConversions._
        if (list.nonEmpty) {
          jedisClient.hmset(redisKey, list.toMap)
        }else{
          jedisClient.del(redisKey)
        }

        if (jedisClient.hgetAll(redisKey).size() >= 0) {
          println("--------------开始--------------------")
          jedisClient.hgetAll(redisKey).foreach(println)
          println("--------------结束--------------------")
        }
        jedisClient.close()
      })
  }
}
