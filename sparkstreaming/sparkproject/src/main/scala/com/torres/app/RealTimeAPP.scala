package com.torres.app

import com.torres.bean.Ads_log
import com.torres.handler.BlackListHandler
import com.torres.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeAPP {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeAPP").setMaster("local[4]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //设置Checkpoint
    //ssc.sparkContext.setCheckpointDir("input")

    //3.创建KafkaDStream
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log",ssc)

    //4.将从kafka中的数据转为样例类 DStream[Ads_log]
    val adsLogDStream: DStream[Ads_log] = KafkaDStream.map(x => {
      val value: String = x.value()
      val str: Array[String] = value.split(" ")
      //timestamp,area,city,userid,adid
      Ads_log(str(0).toLong, str(1), str(2), str(3), str(4))
    })


    //5.根据redis中的黑名单进行过滤
    val filterWithBlackDStream: DStream[Ads_log] = BlackListHandler.filterWithBlackList(adsLogDStream,ssc)
    filterWithBlackDStream.cache()

    //6.需求一：计算用户当日点击单个广告的次数，如果超过100，将其加入黑名单
    BlackListHandler.addBlackList(filterWithBlackDStream)

    //需求二：统计单日每个大区每个城市点击广告总数
//    val dateAreaCityAdToCountDStream: DStream[((String, String, String, String), Long)] = DateAreaCityAdToCountHandler.getDateAreaCityAdToCount(filterWithBlackDStream)
//
//    DateAreaCityAdToCountHandler.writeDateAreaCityAdToCount(dateAreaCityAdToCountDStream)



    //需求三：每天各地区广告点击量Top3
//    dateAreaCityAdToCountDStream
//    DateAreaAdCountTop3Handler.writeDateAreaAdCountTop3(dateAreaCityAdToCountDStream)


    //需求四：每个5秒钟，统计最近2min中各个广告被点击的次数
//    LastHourAdToCountHandler.saveLastHourAdToCount02(filterWithBlackDStream)




    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
