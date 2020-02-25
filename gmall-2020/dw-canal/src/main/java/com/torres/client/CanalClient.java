package com.torres.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.torres.bean.GmallConstants;
import com.torres.utils.KafkaSender;

import java.net.InetSocketAddress;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();

            canalConnector.subscribe("gmall.*");

            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，休息一下");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        handler(tableName, eventType, rowChange);

                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowChange,GmallConstants.GMALL_ORDER_INFO_TOPIC);
        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            sendToKafka(rowChange,GmallConstants.GMALL_ORDER_DETAIL_TOPIC);
        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType) )){
            sendToKafka(rowChange,GmallConstants.GMALL_USER_INFO_TOPIC);
        }
    }

    private static void sendToKafka(CanalEntry.RowChange rowChange,String topic) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //发送至Kafka
            System.out.println(jsonObject.toString());
            try {
                Thread.sleep(new Random().nextInt(3) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            KafkaSender.send(topic, jsonObject.toString());
        }
    }
}
