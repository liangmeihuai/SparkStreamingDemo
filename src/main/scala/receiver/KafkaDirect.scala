package receiver

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by rajat rao on 3/6/16.
 * Simple spark-streaming consumer using kafka dstream
 */
class KafkaDirect(appName:String,
                  brokers: String,
                  topics: Set[String]) {

  val sparkConf = new SparkConf().setAppName(appName).setMaster("local[4]")
  val ssc = new StreamingContext(sparkConf, Seconds(60))
  val kafkaParams = Map("metadata.broker.list" -> brokers)
  val stream =  KafkaUtils.createDirectStream[String, String, StringDecoder,StringDecoder](ssc, kafkaParams,topics)

  def start() = {
    streamData
    ssc.start()
    ssc.awaitTermination()
  }

  def streamData() = {
    stream.foreachRDD{
      rdd=> {
        rdd.foreachPartition{
          p => { p.foreach(ele => println("Element ->" + ele))}
        }
      }
    }
  }
}

object KafkaDirect extends App {
  val directReceiver = new KafkaDirect("streaming","localhost:9092", Set("test"))
  directReceiver.start
}