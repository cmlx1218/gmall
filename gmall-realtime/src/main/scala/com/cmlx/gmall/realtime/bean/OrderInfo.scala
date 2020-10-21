package com.cmlx.gmall.realtime.bean

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/20 14:44
 */
case class OrderInfo(
                      area: String,
                      consignee: String,
                      orderComment: String,
                      var consigneeTel: String,
                      operateTime: String,
                      orderStatus: String,
                      paymentWay: String,
                      userId: String,
                      imgUrl: String,
                      totalAmount: Double,
                      expireTime: String,
                      deliveryAddress: String,
                      createTime: String,
                      trackingNo: String,
                      parentOrderId: String,
                      outTradeNo: String,
                      id: String,
                      tradeBody: String,
                      var createDate: String,
                      var createHour: String,
                      var createHourMinute: String
                    ) {

}

