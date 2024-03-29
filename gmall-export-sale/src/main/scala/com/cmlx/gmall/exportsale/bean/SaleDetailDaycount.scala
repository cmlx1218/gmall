package com.cmlx.gmall.exportsale.bean

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/21 11:25
 */
case class SaleDetailDaycount(
                               user_id: String,
                               sku_id: String,
                               user_gender: String,
                               user_age: Int,
                               user_level: String,
                               sku_price: Double,
                               sku_name: String,
                               sku_tm_id: String,
                               sku_category1_id: String,
                               sku_category2_id: String,
                               sku_category3_id: String,
                               sku_category1_name: String,
                               sku_category2_name: String,
                               sku_category3_name: String,
                               spu_id: String,
                               sku_num: Long,
                               order_count: Long,
                               order_amount: Double,
                               var dt: String
                             ) {
}