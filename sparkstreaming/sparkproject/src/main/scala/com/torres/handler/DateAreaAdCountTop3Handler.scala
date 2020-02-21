package com.torres.handler

import com.torres.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object DateAreaAdCountTop3Handler {
  def writeDateAreaAdCountTop3(dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Long)]):Unit = {

    dateAreaCityAdToCountDStream
      .map{case ((date,area,city,ad),count)=>((date,area,ad),count)} //变换结构
      .reduceByKey(_+_) //wordCount
      //进行分组
      .map{case ((date,area,ad),count)=>((date,area),(ad,count))}
      .groupByKey()
      //分组取前3
      .mapValues(iter => iter.toList.sortWith(_._2>_._2).take(3))
      //变换为json结构
      .mapValues(list =>{
        import org.json4s.JsonDSL._
        JsonMethods.compact(list)
      })
      //进行写库操作
      .foreachRDD(rdd =>{
        rdd.foreachPartition(iter =>{
          val jedisClient: Jedis = RedisUtil.getJedisClient
          //进行批量处理
          iter.toList.groupBy(_._1._1).mapValues(list=>{
            list.map{case ((date,area),top3)=>(area,top3)}
          }).foreach{case (date,list)=>{
            val redisKey = s"top3_ads_per_day:$date"
            import scala.collection.JavaConversions._
            jedisClient.hmset(redisKey,list.toMap)

            if(jedisClient.hgetAll(redisKey).size()>=0){
              println("--------------开始--------------------")
              jedisClient.hgetAll(redisKey).foreach(println)
              println("--------------结束--------------------")
            }
          }}
          jedisClient.close()
        })
      })
  }

  def writeDateAreaAdCountTop301(inputDStream: DStream[((String, String, String, String), Long)]):Unit = {
    inputDStream
      .map{case ((date,area,city,ad),count)=>((date,area,ad),count)}
      .reduceByKey(_+_)
      .map{case((date,area,ad),count)=>((date,area),(ad,count))}
      .groupByKey()
      .mapValues(list=>{
        val listTop3: List[(String, Long)] = list.toList.sortBy(_._2).take(3)
        import org.json4s.JsonDSL._
        JsonMethods.compact(listTop3)
      })
      .foreachRDD(rdd =>{
        rdd.foreachPartition(iter =>{
          val jedisClient: Jedis = RedisUtil.getJedisClient
          iter.toList
            .groupBy(_._1._1)
            .mapValues(list=>{
              list.map{case ((date,area),top3Info)=>(area,top3Info)}
            })
            .foreach{case (date,list)=>{
              val redisKey:String = s"top3_ads_per_day:$date"
              import scala.collection.JavaConversions._
              jedisClient.hmset(redisKey,list.toMap)
            }}
          jedisClient.close()
        })
      })
  }

}
