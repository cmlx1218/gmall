package com.cmlx.gmall.realtime.app

import com.cmlx.gmall.common.constant.GmallConstant
import com.cmlx.gmall.realtime.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/13 15:04
 */
object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    inputDStream.foreachRDD {
      rdd: RDD[ConsumerRecord[String, String]] =>
        println(rdd.map((_: ConsumerRecord[String, String]).value()).collect().mkString("\n"))
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
