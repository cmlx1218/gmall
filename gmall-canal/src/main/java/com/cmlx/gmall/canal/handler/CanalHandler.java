package com.cmlx.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cmlx.gmall.canal.utils.MyKafkaSender;
import com.cmlx.gmall.common.constant.GmallConstant;
import com.google.common.base.CaseFormat;

import java.util.List;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/19 18:52
 */
public class CanalHandler {

    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        // 下单操作
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            // 行集展开
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                // 列集展开
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName() + ":::" + column.getValue());
                    // 下划线转驼峰
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, column.getName());
                    jsonObject.put(propertyName, column.getValue());
                }
                // 每一行发一次kafka
                // 服务器消费kafka命令： bin/kafka-console-consumer.sh --bootstrap-server master:6667,slave1:6667,slave2:6667 --topic GMALL_ORDER --from-beginning
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonObject.toJSONString());
            }

        }
    }

}
