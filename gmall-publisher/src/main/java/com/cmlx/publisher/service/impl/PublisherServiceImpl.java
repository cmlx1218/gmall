package com.cmlx.publisher.service.impl;

import com.cmlx.publisher.common.GmallConstant;
import com.cmlx.publisher.service.IPublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/15 15:30
 */
@Service
public class PublisherServiceImpl implements IPublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) throws IOException {
        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"filter\": {\n" +
                "                 \"term\":{\n" +
                "                      \"logDate\": \"2020-10-15\"\n" +
                "                  }\n" +
                "             }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        // 使用直接写sql的方法如果很复杂的话是不利于维护和可读性的
        // 这里使用工具来构建sql字符串
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        query = searchSourceBuilder.toString();

        //或者
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(new BoolQueryBuilder().filter(new TermQueryBuilder("logDate", date)))
                .sort(SortBuilders.fieldSort("ts")
                        .order(SortOrder.ASC));
        query = sourceBuilder.toString();

        //或者
        /*
          matchQuery：会将搜索词分词，再与目标查询字段进行匹配，若分词中的任意一个词与目标字段匹配上，就可以查询到
          termQuery：不会将搜索词分词，而是作为一个整体与目标字段进行匹配，若完全匹配，则可以查询到
         */
        query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("logDate", date)).mustNot(QueryBuilders.termQuery("mid",111)).toString();

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        SearchResult searchResult = jestClient.execute(search);
        Long total = searchResult.getTotal();
        return total;
    }
}
