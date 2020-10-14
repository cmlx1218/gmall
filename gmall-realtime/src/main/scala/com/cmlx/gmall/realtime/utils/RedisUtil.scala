package com.cmlx.gmall.realtime.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/14 11:12
 */
object RedisUtil {

  var jedisPool: JedisPool = null

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      // 开辟一个新的连接池
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: String = properties.getProperty("redis.port")
      val dataBase: String = properties.getProperty("redis.database")
      val password: String = properties.getProperty("redis.password")
      val timeout: String = properties.getProperty("redis.timeout")
      val maxActive: String = properties.getProperty("redis.max-active")
      val maxTotal: String = properties.getProperty("redis.max-total")
      val maxIdle: String = properties.getProperty("redis.max-idle")
      val minIdle: String = properties.getProperty("redis.min-idle")
      val blockWhenExhausted: String = properties.getProperty("redis.block-when-exhausted")
      val maxWait: String = properties.getProperty("redis.max-wait")
      val testOnBorrow: String = properties.getProperty("redis.test-borrow")
      val jedisPoolConfig = new JedisPoolConfig()

      // 最大连接数
      jedisPoolConfig.setMaxTotal(maxTotal.toInt)
      // 最大空闲数
      jedisPoolConfig.setMaxIdle(maxIdle.toInt)
      // 最小空闲
      jedisPoolConfig.setMinIdle(minIdle.toInt)
      // 忙碌时是否等待
      jedisPoolConfig.setBlockWhenExhausted(blockWhenExhausted.toBoolean)
      // 忙碌时等待时长 毫秒
      jedisPoolConfig.setMaxWaitMillis(maxWait.toInt)
      // 每次获得连接的进行测试
      jedisPoolConfig.setTestOnBorrow(testOnBorrow.toBoolean)

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)

    }
    jedisPool.getResource
  }

}
