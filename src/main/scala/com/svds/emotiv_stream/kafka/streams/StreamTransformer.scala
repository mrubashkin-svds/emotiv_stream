package com.svds.emotiv_stream.kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, ValueMapper}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/** Converts Emotiv CSV to OpenTSDB format */
object StreamTransformer extends App {
  val config = new Properties
  val host = args(0)
  config.put(StreamsConfig.JOB_ID_CONFIG, "com.svds.emotiv_stream.kafka.streams.StreamTransformer")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, s"${host}:9092")
  config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, s"${host}:2181")
  config.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  config.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  config.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  config.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])


  val builder: KStreamBuilder = new KStreamBuilder
  var source = args(1)
  val textLines: KStream[String, String] = builder.stream(source)
  val transformed = textLines.mapValues(new ValueMapper[String, String]() {
    override def apply(record: String) = {
      //val Array(counter, af3, f7, f3, fc5, t7, p7, o1, o2, p8, ti, fc6, f4, f8, af4, gyrox, gyroy, timestamp, funcID, funcValue, marker, syncSignal) = record.split(',')
      val array = record.split(',')
      var af3 = "0"
      if (array.length > 1) { // TODO Avro schema
        af3 = array(1)
      }
      s"put eeg ${System.currentTimeMillis / 1000} ${af3} sensor=af3"
    }
  })
  val sink = args(2)
  transformed.to(sink)
  val streams = new KafkaStreams(builder, config)
  streams.start()
}