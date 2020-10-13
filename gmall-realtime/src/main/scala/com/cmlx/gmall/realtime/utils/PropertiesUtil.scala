package com.cmlx.gmall.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: CMLX
 * @Description: 读取配置文件工具类
 * @Date: create in 2020/10/13 17:01
 */
object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertiesName: String): Properties = {
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    properties
  }

}
