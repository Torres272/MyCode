import com.torres.bean.GmallConstants
import com.torres.util.MyKafkaUtil
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


    val startupStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.KAFKA_TOPIC_STARTUP))

    startupStream.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
