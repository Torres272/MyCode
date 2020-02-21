package com.torres.handler

import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {


  //时间格式化
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //黑名单的RedisKey
  private val blackListRedisKey = "BlackList"


  def filterWithBlackList(adsLog: DStream[Ads_log], ssc: StreamingContext): DStream[Ads_log] = {
    adsLog.transform(rdd => {

      println(s"filterWithBlackList分区的数量：${rdd.partitions.size}")

      //获取一次黑名单，将数据存放在广播变量中
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val userIdInBlackList: util.Set[String] = jedisClient.smembers(blackListRedisKey)
      jedisClient.close()
      val uidsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(userIdInBlackList)
      rdd.filter(adsLog => !uidsBC.value.contains(adsLog.userid))



      //      //每个分区创建一个连接，但是只取一次数据
      //      rdd.mapPartitions(iter=>{
      //        println("-------------------------------------")
      //        val jedisClient: Jedis = RedisUtil.getJedisClient
      //        val userIdInBlackList: util.Set[String] = jedisClient.smembers(blackListRedisKey)
      //        jedisClient.close()
      //        iter.filter(ads_log => !userIdInBlackList.contains(ads_log.userid))
      //      })

      //      //每个分区创建一个连接，每次过滤都读取数据一次数据
      //      rdd.mapPartitions(iter => {
      //        //创建客户端
      //        val jedisClient: Jedis = RedisUtil.getJedisClient
      //        //进行过滤
      //        val ads_logs: Iterator[Ads_log] = iter.filter(adsLog => !jedisClient.sismember(blackListRedisKey, adsLog.userid))
      //        //关闭客户端
      //        jedisClient.close()
      //        //返回值
      //        ads_logs
      //      })

    })
  }

  //方法：把人加进黑名单
  def addBlackList(adsLogDStream: DStream[Ads_log]): Unit = {
    //格式转换
    val dateUserAdToOneDStream: DStream[((String, String, String), Long)] = adsLogDStream.map(x => ((sdf.format(new Date(x.timestamp)), x.userid, x.adid), 1L))
    dateUserAdToOneDStream.foreachRDD(rdd=>println("dateUserAdToOneDStream"+rdd.partitions.size))
    //统计次数
    val dateUserAdToCount: DStream[((String, String, String), Long)] = dateUserAdToOneDStream.reduceByKey(_ + _)
    dateUserAdToCount.foreachRDD(rdd=>println("dateUserAdToCount"+rdd.partitions.size))

    //加入黑名单
    dateUserAdToCount.foreachRDD(x => {
      println(s"dateUserAdToCount分区的数量：${x.partitions.size}")
      x.foreachPartition(par => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        par.foreach {
          case ((date, user, ad), count) => {
            val redisKey = s"user-ad-$date"
            val hashKey = s"user$user-ad$ad"

            //进行类加
            jedisClient.hincrBy(redisKey, hashKey, count)

            val countNow: Long = jedisClient.hget(redisKey, hashKey).toLong
            //println(s"$hashKey:$countNow")
            //获取类加的值
            if (countNow >= 10000) {
              jedisClient.sadd(blackListRedisKey, user)
            }
          }
        }
        jedisClient.close()
      })
    })

  }


  //把人加入黑名单
  def addBlackList01(adsLogDStream: DStream[Ads_log]): Unit = {
    adsLogDStream.map(x => ((sdf.format(x.timestamp), x.userid, x.adid), 1L)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          val jedisClient: Jedis = RedisUtil.getJedisClient
          iter.foreach {
            case ((date, userId, ad), count) => {
              //拼接redisKey然后更新每个用户对每个广告的点击次数
              val redisKey: String = s"user-ad-$date"
              val hashKey = s"user$userId-ad$ad"
              jedisClient.hincrBy(redisKey, hashKey, count)

              //判断点击次数是否达到100，将其加入黑名单
              if (jedisClient.hget(redisKey, hashKey).toLong >= 100L) {
                jedisClient.sadd(blackListRedisKey, userId)
              }
            }
          }
          jedisClient.close()
        })
      })
  }
}
