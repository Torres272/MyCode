package com.torres.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler1 {
  //黑名单key
  private val blackListKey = "blackList"
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    //方法一
    adsLogDStream.transform(
      rdd => rdd.mapPartitions(
        itr => {
          //获取redis连接
          val client: Jedis = RedisUtil.getJedisClient
          //对redis中的黑名单进行过滤
          val ads_logs: Iterator[Ads_log] = itr.filter(data => {
            !client.sismember(blackListKey, data.userid)
          })
          //关闭redis连接
          client.close()
          ads_logs
        }
      )
    )
  }


  def addBlackList(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    //格式化时间类
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    //转换结构为(date,user,ad),1L
    val dateUserAdToOneDStream: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(data => {
      ((sdf.format(new Date(data.timestamp)), data.userid, data.adid), 1L)
    })
    //按照key做聚合操作
    val dateUserAdToCountDStream: DStream[((String, String, String), Long)] = dateUserAdToOneDStream.reduceByKey(_+_)

    //连接redis，将结果累加到redis中，并检查符合条件的用户，加入到黑名单中
    dateUserAdToCountDStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          itr=>{
            //建立redis连接
            val client: Jedis = RedisUtil.getJedisClient
            //写redis操作
            itr.foreach{
              case ((date,user,ad),count)=>{
                //redis key
                val redisKey = s"date:${date}"
                val hashKey = s"user:${user}-ad:${ad}"
                //写入redis
                client.hincrBy(redisKey,hashKey,count)

                //判断用户是否满足条件，加入黑名单
                if(client.hget(redisKey,hashKey).toLong >= 30L){
                  client.sadd(blackListKey,user)
                }
              }
            }
            //关闭redis连接
            client.close()
          }
        )
      }
    )
  }
}
