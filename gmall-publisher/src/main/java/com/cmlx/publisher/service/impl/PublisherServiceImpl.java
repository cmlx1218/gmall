package com.cmlx.publisher.service.impl;

import com.cmlx.publisher.common.GmallConstant;
import com.cmlx.publisher.service.IPublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        // 单独只做查询的时候可以只用QueryBuilders，但是要做其他操作，例如：聚合、排序的时候就需要用到SearchSourceBuilders
        query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("logDate", date)).mustNot(QueryBuilders.termQuery("mid", 111)).toString();

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        SearchResult searchResult = jestClient.execute(search);
        Long total = searchResult.getTotal();
        return total;
    }

    @Override
    public Map getDauHourMap(String date) throws IOException {
        String query = new SearchSourceBuilder()
                .query(new BoolQueryBuilder().filter(new TermQueryBuilder("logDate", date)))
                .aggregation(AggregationBuilders.terms("groupby_logHour").field("logHour").size(24))//默认只会返回10个分组信息，所以需要指定size
                .toString();

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        SearchResult searchResult = jestClient.execute(search);
        List<TermsAggregation.Entry> groupby_logHour = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
        Map dauHourMap = new HashMap();
        for (TermsAggregation.Entry entry : groupby_logHour) {
            String key = entry.getKey();
            Long count = entry.getCount();
            dauHourMap.put(key, count);
        }
        return dauHourMap;
    }
}
