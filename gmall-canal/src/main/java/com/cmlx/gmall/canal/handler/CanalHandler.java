package com.cmlx.gmall.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;

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
                // 列集展开
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName() + ":::" + column.getValue());

                }
            }

        }
    }

}
