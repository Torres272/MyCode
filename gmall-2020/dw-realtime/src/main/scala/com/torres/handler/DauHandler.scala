package com.torres.handler

import java.text.SimpleDateFormat
import java.util

import com.torres.bean.StartUpLog
import com.torres.util.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable

object DauHandler {


  //时间格式化
  val sdf = new SimpleDateFormat("yyyy-MM-dd")


  //通过redis过滤各个批次间的重复数据
  def filterDataByRedis(ssc: StreamingContext, startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //通过redis的值进行过滤------同时过滤完也是返回值
    startLogDStream.transform(rdd => {

      //创建redis连接，获取今天和昨天的redisKey
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val currentTime: Long = System.currentTimeMillis()
      val currentDate: String = sdf.format(currentTime)
      val yesterdayDate: String = sdf.format(currentTime - 24 * 60 * 60 * 1000L)
      val redisKeyToday: String = s"dau:$currentDate"
      val redisKeyYesterday: String = s"dau:$yesterdayDate"

      //定义容器
      val redisDataMap: mutable.Map[String, util.Set[String]] = mutable.Map[String, util.Set[String]]()

      //获取昨天和今天的Redis列表,并加入map
      val todayRedisSet: util.Set[String] = jedisClient.smembers(redisKeyToday)
      //println(s"today:${todayRedisSet.size()}")
      val yesterdayRedisSet: util.Set[String] = jedisClient.smembers(redisKeyYesterday)
      //println(s"yesterday:${yesterdayRedisSet.size()}")
      redisDataMap += (redisKeyToday -> todayRedisSet)
      redisDataMap += (redisKeyYesterday -> yesterdayRedisSet)

      //创建广播变量
      val redisDataBroadcast: Broadcast[mutable.Map[String, util.Set[String]]] = ssc.sparkContext.broadcast(redisDataMap)

      //关闭连接
      jedisClient.close()

      //进行过滤
      rdd.mapPartitions(iter => {
        iter.filter(log => {
          val logDate: String = log.logDate
          val redisKey = s"dau:$logDate"
          val filterSet: Option[util.Set[String]] = redisDataBroadcast.value.get(redisKey)

//          import scala.collection.JavaConversions._
//          val set: util.HashSet[String] = new util.HashSet[String]()
//          val strings: util.Set[String] = redisDataBroadcast.value.getOrElse(redisKey, set )

          !filterSet.get.contains(log.mid)
        })
      })
    })
  }

  //同批次进行过滤
  def filterDataByBatch(filterDataByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    //过滤数据，变换结构，相同数据分组，然后拿时间最早的一个，同时也是返回值
    filterDataByRedisDStream
      .map(log => ((log.mid, log.logDate), log))
      .groupByKey()
      .flatMap { case ((_, _), log) =>
        log.toList.sortWith(_.ts < _.ts).take(1)
      }
  }

  //写入redis
  def saveDataToRedis(filterDataByBatchDStream: DStream[StartUpLog]) = {
    filterDataByBatchDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //redis客户端
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //写入redis
        iter.foreach(log => {
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //关闭
        jedisClient.close()
      })
    })

  }

}
