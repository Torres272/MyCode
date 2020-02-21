import java.sql.Date
import java.text.SimpleDateFormat

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdToCountHandler {

  //时间格式化对象
  private val sdf = {
    new SimpleDateFormat("HH:mm")
  }

  //RedisKey
  private val redisKey = "last_hour_ads_click"

  /**
   * 每隔5秒钟统计最近2min中各个广告被点击的次数
   *
   * @param filterAdsLogDSteam 根据黑名单信息过滤后的数据集
   */
  def saveLastHourAdToCountToRedis(filterAdsLogDSteam: DStream[Ads_log]): Unit = {

    //1.开窗(2min)
    val windowAdsLogDSteam: DStream[Ads_log] = filterAdsLogDSteam.window(Minutes(1))

    //2.转换数据结构adsLog=>((ad,hm),1L)
    val adHmToOne: DStream[((String, String), Long)] = windowAdsLogDSteam.map(adsLog => {

      //取出时分
      val hm: String = sdf.format(new Date(adsLog.timestamp))

      ((adsLog.adid, hm), 1L)
    })

    //3.统计总数((ad,hm),1L)=>((ad,hm),count)
    val adHmToCount: DStream[((String, String), Long)] = adHmToOne.reduceByKey(_ + _)

    //4.按照广告id分组 ((ad,hm),count) =>(ad,Iter[(hm,count)...])
    val adToHmCountIter: DStream[(String, Iterable[(String, Long)])] = adHmToCount.map { case ((ad, hm), count) =>
      (ad, (hm, count))
    }.groupByKey()

    //5.将时分及点击次数转换为JSON格式
    val adToHmCountJson: DStream[(String, String)] = adToHmCountIter.mapValues(iter => {
      //按照时分排序
      val sortedList: List[(String, Long)] = iter.toList.sortWith(_._1 < _._1)
      //转换为JSON字符串
      import org.json4s.JsonDSL._
      JsonMethods.compact(sortedList)
    })

    //6.写入Redis(???)
    adToHmCountJson.foreachRDD(rdd => {

      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //将Redis数据删除
      jedisClient.del(redisKey)
      //关闭连接
      jedisClient.close()

      rdd.foreachPartition(iter => {

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //写库操作（单条数据写入）=> 批量插入
        iter.foreach { case (ad, json) =>
          jedisClient.hset(redisKey, ad, json)
        }
        //关闭连接
        jedisClient.close()
      })
    })
  }
}

