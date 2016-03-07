#SparkStreamingDemo using Kafka DStream

Spark-streaming consumer app using kafka direct stream approach (http://spark.apache.org/docs/latest/streaming-kafka-integration.html)
Streaming app queries kafka with a range of offset data to be fetched for a given batch duration.
Stream consists of batches of RDDs. 1 kafka partition is mapped to 1 RDD partition
Demo Producer creates a bunch of KeyedMessage for kafka
Streaming consumer receives these message by iterating over the stream of RDDs

#Run
Producer
java -classpath ./target/dependency-jars/*:./target/demo-1.0-SNAPSHOT.jar producer.MyProducer

Consumer
java -classpath ./target/dependency-jars/*:./target/sparkstreamingkafkademo-1.0-SNAPSHOT.jar receiver.KafkaDirect
