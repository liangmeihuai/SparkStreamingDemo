package producer

import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by rajat rao on 3/6/16.
 *
 * Basic Kafka Producer
 */

class MyProducer {
  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "sync")
  props.put("batch.num.messages", "1")
  props.put("message.send.max.retries", "10")
  props.put("request.required.acks", "-1")
  val config = new ProducerConfig(props)
  val myProducer = new Producer[String, String](config)
  val topic = "test"

  def send(): Unit = {
    val inputMessages: List[KeyedMessage[String, String]] = (101 to 200).map(i => new KeyedMessage[String, String](topic, s"key-$i", s"message $i")).toList
    println("Sending" + inputMessages.size + "Messages")
    myProducer.send(inputMessages: _*)
  }

  def close():Unit ={
    myProducer.close()
  }
}

object MyProducer extends App {
  val producer = new MyProducer
  producer.send()
  producer.close()
}