import com.torres.bean.CityRemark

object Test01 {
  def main(args: Array[String]): Unit = {
    val aa = CityRemark("其他",0.0)
    println(aa)
    println(aa=="其他:0.00%")
    println(aa.toString=="其他:0.00%")
  }
}
