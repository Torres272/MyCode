package com.torres.sql

import com.torres.udaf.AreaClickUDAF
import org.apache.spark.sql.SparkSession

object AreaTop3ProductApp {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HelloWord")
      .enableHiveSupport()
      .getOrCreate()

    //导入隐式转换
    spark.udf.register("city_remark",new AreaClickUDAF)

    spark.sql("use sparkpractice")

    spark.sql(
      """
        |SELECT u.*,c.area area,p.product_name product_name ,c.city_name city_name
        |from(select * from user_visit_action where click_product_id>-1) u  left JOIN city_info c on c.city_id = u.city_id
        |left join product_info p on p.product_id = u.click_product_id
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select area,product_name,count(*) ct, city_remark(city_name) city_remark
        |from
        |t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")//.show(20,truncate = false)

    spark.sql(
      """
        |select * ,rank() over(partition by area order by ct desc) rk
        |from
        |t2
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select area,product_name, ct, city_remark
        |from t3
        |where rk <=3
        |""".stripMargin).show(100,truncate = false)



    //关闭
    spark.close()
  }
}
