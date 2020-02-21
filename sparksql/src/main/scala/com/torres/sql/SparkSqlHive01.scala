package com.torres.sql

import org.apache.spark.sql.SparkSession

object SparkSqlHive01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQLHive2")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //2.直接查询Hive中的数据
    //spark.sql("select * from student").show()

    spark.sql(
      """
        |select *
        |from student
      """.stripMargin).show()

    val map:  Map[String, String] = Map[String,String]()



    //3.关闭连接
    spark.stop()
  }
}
