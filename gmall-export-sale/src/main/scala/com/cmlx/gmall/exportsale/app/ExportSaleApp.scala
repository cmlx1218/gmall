package com.cmlx.gmall.exportsale.app

import com.cmlx.gmall.common.constant.GmallConstant
import com.cmlx.gmall.common.util.MyEsUtil
import com.cmlx.gmall.exportsale.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/21 11:18
 */
object ExportSaleApp {

  def main(args: Array[String]): Unit = {

    var date = "";
    if (args != null && args.length > 0) {
      date = args(0)
    } else {
      // 去当前时间日期
      date = "2020-10-21"
    }
    val sparkConf: SparkConf = new SparkConf().setAppName("ExportSale").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 最好不要用 * 把字段全部写出来，方便转换字段类型和字段名对应到case class
    // 差一个隐式转换，导包
    import sparkSession.implicits._
    //    sparkSession.sql("select * from dws_sale_detail_daycount").as[SaleDetailDaycount].rdd
    val saleDetailDayCountRDD: RDD[SaleDetailDaycount] = sparkSession.sql("select user_id,sku_id,user_gender," +
      "cast(user_age as int) user_age,user_level,cast(sku_price as double),sku_name,sku_tm_id, sku_category3_id,sku_category2_id," +
      "sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count," +
      "cast(order_amount as double) order_amount,dt from dws_sale_detail_daycount where dt='" + date + "'").as[SaleDetailDaycount].rdd

    saleDetailDayCountRDD.foreachPartition {
      saleItr: Iterator[SaleDetailDaycount] => {

        val listBuffer: ListBuffer[SaleDetailDaycount] = ListBuffer()
        // 这里是一天的数据，可能量会太大
        for (saleDetail <- saleItr) {
          listBuffer += saleDetail
          if (listBuffer.size == 100) {
            MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE, listBuffer.toList)
            listBuffer.clear()
          }
        }
        if (listBuffer.nonEmpty) {
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE, listBuffer.toList)
        }
      }
    }



  }

}

// 分词器
/*
PUT gmall2019_sale_detail
{
  "mappings": {
  "_doc":{
  "properties":{
  "user_id":{
  "type":"keyword"
},
  "sku_id":{
  "type":"keyword"
},
  "user_gender":{
  "type":"keyword"
},
  "user_age":{
  "type":"short"
},
  "user_level":{
  "type":"keyword"
},
  "sku_price":{
  "type":"double"
},
  "sku_name":{
  "type":"text",
  "analyzer": "ik_max_word"
},
  "sku_tm_id ":{
  "type":"keyword"
},
  "sku_category3_id":{
  "type":"keyword"
},
  "sku_category2_id":{
  "type":"keyword"
},
  "sku_category1_id":{
  "type":"keyword"
},
  "sku_category3_name":{
  "type":"text",
  "analyzer": "ik_max_word"
},
  "sku_category2_name":{
  "type":"text",
  "analyzer": "ik_max_word"
},
  "sku_category1_name":{
  "type":"text",
  "analyzer": "ik_max_word"
},
  "spu_id":{
  "type":"keyword"
},
  "sku_num":{
  "type":"long"
},
  "order_count":{
  "type":"long"
},
  "order_amount":{
  "type":"long"
},
  "dt":{
  "type":"keyword"
}
}
}
}
}*/
