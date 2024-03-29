package com.cmlx.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.cmlx.gmall.canal.handler.CanalHandler;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @Author: CMLX
 * @Description:
 * @Date: create in 2020/10/19 17:44
 */
public class CanalApp {

    public static void main(String[] args) {

        // 创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("master", 11111), "example", "", "");
        // 循环获取
        while (true) {
            //连接、订阅、抓取数据
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                try {
                    System.out.println("没有数据，休息一会儿...");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 判断事件类型,只处理行变化
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        // 把数据集反序列化
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        // 获得行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 获取操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 获取表名
                        String tableName = entry.getHeader().getTableName();

                        CanalHandler.handle(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }
}
