package com.cmlx.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.cmlx.gmall.common.constant.GmallConstant
import com.cmlx.gmall.realtime.bean.StartUpLog
import com.cmlx.gmall.realtime.utils.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

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

    //    inputDStream.foreachRDD {
    //      rdd: RDD[ConsumerRecord[String, String]] =>
    //        println(rdd.map((_: ConsumerRecord[String, String]).value()).collect().mkString("\n"))
    //    }

    // 转换处理
    val startUpLogDStream: DStream[StartUpLog] = inputDStream.map {
      record: ConsumerRecord[String, String] =>
        val jsonStr: String = record.value()
        val startuplog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        val date = new Date(startuplog.ts)
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr: Array[String] = dateStr.split(" ")
        startuplog.logDate = dateArr(0)
        startuplog.logHour = dateArr(1).split(":")(0)
        startuplog.logHourMinute = dateArr(1)

        startuplog
    }

    // 【批次间过滤】
    // 利用redis进行去重过滤

    //    val curDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    //    val client: Jedis = RedisUtil.getJedisClient
    //    val key: String = "dau:"+curDate
    //    val dauSet: util.Set[String] = client.smembers(key)
    //    // 数据在driver中,设置广播变量就可以在各个executor中使用了【没在算子driver中,这里只会执行一次不会周期性执行,而redis里面的内容是实时变化的】
    //    val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
    //    startUpLogDStream.filter { startUpLog: StartUpLog =>
    //      val dauSet: util.Set[String] = dauBC.value
    //      !dauSet.contains(startUpLog.mid)
    //    }

    val filterDStream: DStream[StartUpLog] = startUpLogDStream.transform { rdd: RDD[StartUpLog] =>
      println("过滤前：" + rdd.count())
      val curDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      val client: Jedis = RedisUtil.getJedisClient
      val key: String = "dau:" + curDate
      val dauSet: util.Set[String] = client.smembers(key)
      // 数据在driver中,设置广播变量就可以在各个executor中使用了【在transform算子driver中,会周期性执行,而redis里面的内容是实时变化的】
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      val filterRDD: RDD[StartUpLog] = rdd.filter { startUpLog: StartUpLog =>
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startUpLog.mid)
      }
      println("过滤后：" + rdd.count())
      filterRDD
    }

    // 【批次内过滤】
    // 我们是按批次处理的，如果当前五秒有重复数据的话就还是会出现重复数据
    // 去重思路：把相同的mid数据分成一个组，每组取第一个
    val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map((startUpLog: StartUpLog) => (startUpLog.mid, startUpLog)).groupByKey()
    val distinctDStream: DStream[StartUpLog] = groupByMidDStream.flatMap { case (mid, startUpLogItr) =>
      startUpLogItr.take(1)
    }



    // 保存到redis中
    distinctDStream.foreachRDD { rdd: RDD[StartUpLog] => //driver中执行,可能在不同服务器执行

      rdd.foreachPartition { startUpLogItr: Iterator[StartUpLog] => //在executor中执行，但是这里用的Partition就可以减少获取jedisClient和关闭的次数
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val list: List[StartUpLog] = startUpLogItr.toList
        for (startUpLog <- list) {
          val key: String = "dau:" + startUpLog.logDate
          val value: String = startUpLog.mid
          jedisClient.sadd(key, value)
        }
        jedisClient.close()
      }

      //      rdd.foreach { startUpLog: StartUpLog =>   //executor中执行,一台服务器上执行,所以JedisClient只能在循环内获取,但是这样每存一次数据就得获取JedisClient显然是不可取的
      //        val jedisClient: Jedis = RedisUtil.getJedisClient
      //        val key: String = "dau:" + startUpLog.logDate
      //        val value: String = startUpLog.mid
      //        jedisClient.sadd(key, value)
      //        jedisClient.close()

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
