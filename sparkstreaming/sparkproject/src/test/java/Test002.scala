import java.sql.Date
import java.text.SimpleDateFormat

import com.torres.bean.Ads_log
import com.torres.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdToCountHandler {


  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //定义更新操作的函数
  val updateFunc: (Seq[Long], Option[Long]) => Some[Long] = (seq: Seq[Long], state: Option[Long]) => {
    val sum: Long = seq.sum
    val lastSum: Long = state.getOrElse(0L)
    Some(sum + lastSum)
  }

  /**
   *
   * @param dateAreaCityAdToCount
   * @return
   */
  def saveDateAreaCityAdToCountToRedis(dateAreaCityAdToCount: DStream[((String, String, String, String), Long)]): Unit = {
    //写入redis  hash(redisKey,hashKey,value)=>
    // (date-area-city-ad-2020-02-12,$area-$city-$ad,count)
    //foreachRDD
    dateAreaCityAdToCount.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.方案一：写库操作
        //        iter.foreach { case ((date, area, city, ad), count) =>
        //          val redisKey = s"date-area-city-ad-$date"
        //          val hashKey = s"$area-$city-$ad"
        //          jedisClient.hset(redisKey, hashKey, count.toString)
        //        }

        //方案二：批量操作
        //按照时间分组  (date,List[(date,area,city,ad),count])
        val dateToInfoMap: Map[String, List[((String, String, String, String), Long)]] = iter.toList.groupBy(_._1._1)

        //==>(date,ListMapIter[($area-$city-$ad)->count,....*5])
        val dateToListMap: Map[String, List[(String, String)]] = dateToInfoMap.mapValues(list => {
          list.map { case ((_, area, city, ad), count) =>
            (s"$area-$city-$ad", count.toString)
          }
        })

        //写入Redis
        dateToListMap.foreach { case (date, list) =>
          val redisKey = s"date-area-city-ad-$date"
          import scala.collection.JavaConversions._
          jedisClient.hmset(redisKey, list.toMap)
        }

        //c.关闭连接
        jedisClient.close()

      })

    })
  }

  /**
   * 统计单日每个大区每个城市点击广告总数，并将结果写入Redis
   *
   * @param filterAdsLogDSteam 根据黑名单信息过滤之后的数据集
   */
  def getDateAreaCityAdToCount(filterAdsLogDSteam: DStream[Ads_log]): DStream[((String, String, String, String), Long)] = {

    //1.转换结构  Ads_log=>((date,area,city,ad),1L)
    //map
    val dateAreaCityAdToOne: DStream[((String, String, String, String), Long)] = filterAdsLogDSteam.map(adsLog => {
      val date: String = sdf.format(new Date(adsLog.timestamp))
      ((date, adsLog.area, adsLog.city, adsLog.adid), 1L)
    })

    //2.计算总点击数 ((date,area,city,ad),1L)=>((date,area,city,ad),count)
    //reduceByKey(updateStateByKey)
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = dateAreaCityAdToOne.updateStateByKey(updateFunc)

    dateAreaCityAdToCount

  }

}