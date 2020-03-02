import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object test01 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//        val value: DataStream[Person] = env.fromElements(new Person(),new Person())
//        val value1: DataStream[People] = env.fromElements(People(1,"a"),People(2,"kk"))
//        env.fromCollection()
        val readDS: DataStream[String] = env.readTextFile("input")
        readDS.map(new MyMapFunction01())
        env.readTextFile("input")
          .flatMap(_.split(" "))
          .map(new MyMapFunction01).print()




        env.readTextFile("input/num.txt")
          .map(x=>new Integer(x))
          .map(new MyMapFunction02())


        env.execute("mySource")
    }
}

class MyMapFunction extends MapFunction[String, String] {
    override def map(s: String): String = {
        s + new Random().nextInt(9)
    }
}

case class People(id:Int,name:String)