package com.torres.handler

import java.text.SimpleDateFormat

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdToCountHandler {

  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")


  val updateFunc: (Seq[Long], Option[Long]) => Option[Long] = (seq, state) => Some(seq.sum + state.getOrElse(0L))


  def getDateAreaCityAdToCount(adsLogDStream: DStream[Ads_log]): DStream[((String, String, String, String), Long)] = {
    val value: DStream[((String, String, String, String), Long)] = adsLogDStream
      .map(adsLog => ((sdf.format(adsLog.timestamp), adsLog.area, adsLog.city, adsLog.adid), 1L))
      .updateStateByKey(updateFunc)
    value
  }

  def writeDateAreaCityAdToCount(adsLogDStream:DStream[((String, String, String, String), Long)]):Unit ={
    adsLogDStream.foreachRDD(rdd => {
      println(s"writeDateAreaCityAdToCount分区的数量：${rdd.partitions.size}")
      rdd.foreachPartition(iter => {
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //toList =》分组 =》变换结构 =》遍历并写入redis
        iter.toList
          .groupBy(_._1._1)
          .mapValues(x => x.map { case ((date, area, city, ad), count) => (s"$area-$city-$ad", count.toString) })
          .foreach { case (date, list) => {
            val redisKey = s"date-area-city-ad-$date"
            import scala.collection.JavaConversions._
            jedisClient.hmset(redisKey, list.toMap)
          }
          }
        jedisClient.close()

      })
    })
  }


}
