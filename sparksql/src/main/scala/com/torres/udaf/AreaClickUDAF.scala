package com.torres.udaf

import com.torres.bean.CityRemark
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AreaClickUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("city", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("city_count", MapType(StringType, LongType))
    :: StructField("total_count", LongType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val cityName: String = input.getString(0)
    buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
    buffer(1) = buffer.getLong(1) + 1L


  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)

    buffer1(0) = map1.foldLeft(map2) {
      case (map, (k, v)) => map + (k -> (map.getOrElse(k, 0L) + v))
    }

    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  override def evaluate(buffer: Row): Any = {
    val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val total: Long = buffer.getLong(1)
    var cityRadio: List[CityRemark] = map.toList.sortBy(-_._2).take(2).map {
      case (city, value) => CityRemark(city, value / total.toDouble)
    }
    if(map.size>2){
      cityRadio = cityRadio :+ CityRemark("其他",cityRadio.foldLeft(1D)(_ - _.ratio))
    }
    cityRadio.mkString(",")
  }
}
