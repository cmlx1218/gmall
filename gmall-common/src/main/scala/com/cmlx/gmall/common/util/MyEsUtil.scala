package com.cmlx.gmall.common.util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import io.searchbox.indices.CreateIndex
import io.searchbox.indices.mapping.PutMapping
import org.elasticsearch.index.query.{QueryBuilders, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder


/**
 * @Author: CMLX
 * @Description: 操作ES工具类
 * @Date: create in 2020/10/14 18:08
 */
object MyEsUtil {

  private val ES_HOST = "http://master"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端
   *
   * @return
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   *
   * @param client
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) {
      try {
        client.shutdownClient()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  /**
   * 测试ES
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    val source = "{\n  \"name\":\"li4\",\n  \"age\":456,\n  \"amount\": 250.1,\n  \"phone_num\":\"138***2123\"\n}"
    val index: Index = new Index.Builder(source).index("gmall_test").`type`("_doc").build()
    jest.execute(index)
    close(jest)
  }

  /**
   * 批量插入ES
   *
   * @param indexName
   * @param list
   */
  def indexBulk(indexName: String, list: List[Any]): Unit = {
    val jest: JestClient = getClient
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultIndex("_doc")
    for (doc <- list) {
      val index: Index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }

    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems

    // 快捷键 priv
    println(s"保存 = ${items.size()}")
    close(jest)
  }


  /**
   * 创建索引
   *
   * @param indexName
   */
  def createIndex(indexName: String): Unit = {
    val jest: JestClient = getClient
    jest.execute(new CreateIndex.Builder(indexName).build())
  }


  /**
   * 创建映射
   *
   * @param indexName
   * @param typeName
   */
  def createIndexMapping(indexName: String, typeName: String): Unit = {
    val source: String = "{\"" + typeName + "\":{\"properties\":{" +
      "\"author\":{\"type\":\"string\",\"index\":\"not_analyzed\"}" +
      ",\"title\":{\"type\":\"string\"}" +
      ",\"content\":{\"type\":\"string\"}" +
      ",\"price\":{\"type\":\"string\"}" +
      ",\"view\":{\"type\":\"string\"}" +
      ",\"tag\":{\"type\":\"string\"}" +
      ",\"date\":{\"type\":\"date\",\"format\":\"yyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\"}" +
      "}}}";
    val putMapping: PutMapping = new PutMapping.Builder(indexName, typeName, source).build()
    val jest: JestClient = getClient
    jest.execute(putMapping)
  }


  /**
   * 单值完全匹配
   *
   * @param indexName
   * @param fieldName
   * @param fieldValue
   * @param size
   * @param from
   * @return
   */
  def termQuery(indexName: String, fieldName: String, fieldValue: String, size: Int, from: Int): SearchResult = {
    val searchSourceBuilder = new SearchSourceBuilder
    val queryBuilder: TermQueryBuilder = QueryBuilders.termQuery(fieldName, fieldValue)
    searchSourceBuilder.query(queryBuilder)
    searchSourceBuilder.size(size)
    searchSourceBuilder.from(from)
    val query: String = searchSourceBuilder.toString

    val search: Search = new Search.Builder(query).addIndex(indexName).addType(indexName).build()

    val jest: JestClient = getClient
    val result: SearchResult = jest.execute(search)
    result
  }


}











