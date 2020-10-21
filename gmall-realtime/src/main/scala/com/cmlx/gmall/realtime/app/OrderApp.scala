package com.cmlx.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.cmlx.gmall.common.constant.GmallConstant
import com.cmlx.gmall.common.util.MyEsUtil
import com.cmlx.gmall.realtime.bean.OrderInfo
import com.cmlx.gmall.realtime.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: CMLX
 * @Description: 订单相关
 * @Date: create in 2020/10/20 12:25
 */
object OrderApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 保存到es
    // 数据脱敏  补时间戳
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, streamingContext)
    val orderInfoDStream: DStream[OrderInfo] = inputDStream.map {
      record: ConsumerRecord[String, String] => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        val telSplit: (String, String) = orderInfo.consigneeTel.splitAt(4)
        orderInfo.consigneeTel = telSplit._1 + "********";

        val datetimeArr: Array[String] = orderInfo.createTime.split(" ")
        orderInfo.createDate = datetimeArr(0)
        val timeArr: Array[String] = datetimeArr(1).split(":")
        orderInfo.createHour = timeArr(0)
        orderInfo.createHourMinute = timeArr(1)
        orderInfo
      }
    }
    // 增加一个字段0或1, 标识该订单是否是该用户首次下单【效仿之前的日活，用redis做去重是一个道理】

    // 保存数据到es
    orderInfoDStream.foreachRDD { rdd: RDD[OrderInfo] =>
      rdd.foreachPartition { orderItr: Iterator[OrderInfo] =>
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_ORDER, orderItr.toList)
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
