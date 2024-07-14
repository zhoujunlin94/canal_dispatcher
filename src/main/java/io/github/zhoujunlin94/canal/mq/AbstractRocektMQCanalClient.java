package io.github.zhoujunlin94.canal.mq;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.setting.Setting;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import io.github.zhoujunlin94.canal.BaseCanalClient;
import io.github.zhoujunlin94.canal.pojo.CanalDispatcherConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * @author zhoujunlin
 * @date 2024/7/14 17:03
 */
@Slf4j
public abstract class AbstractRocektMQCanalClient extends BaseCanalClient {

    private static final Setting ROCKET_MQ_SETTING = new Setting("rocketmq.setting");

    private static final MessageQueueSelector MESSAGE_QUEUE_SELECTOR = new SelectMessageQueueByHash();

    private static final DefaultMQProducer DEFAULT_MQ_PRODUCER;

    static {
        DEFAULT_MQ_PRODUCER = new DefaultMQProducer();
        DEFAULT_MQ_PRODUCER.setNamesrvAddr(ROCKET_MQ_SETTING.get("rocketmq.dispatcher.nameserver"));
        DEFAULT_MQ_PRODUCER.setProducerGroup(ROCKET_MQ_SETTING.get("rocketmq.dispatcher.group"));
        try {
            DEFAULT_MQ_PRODUCER.start();
        } catch (MQClientException e) {
            log.warn("start dispatcher rocket mq failed", e);
        }
    }

    protected String getRocketMQCanalNameServer() {
        return ROCKET_MQ_SETTING.get("rocketmq.canal.nameserver");
    }

    protected String getRocketMQCanalTopic() {
        return ROCKET_MQ_SETTING.get("rocketmq.canal.topic");
    }

    protected String getRocketMQCanalGroup() {
        return ROCKET_MQ_SETTING.get("rocketmq.canal.group");
    }

    @Override
    protected void handleEntry(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry, e);
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                    continue;
                }
                String databaseName = entry.getHeader().getSchemaName();
                String tableName = entry.getHeader().getTableName();

                CanalDispatcherConfig canalDispatcherConfig = getCanalDispatcherConfig(databaseName, tableName);
                if (Objects.isNull(canalDispatcherConfig)) {
                    continue;
                }
                JSONObject canalData = new JSONObject().fluentPut("databaseName", databaseName).fluentPut("tableName", tableName)
                        .fluentPut("eventType", eventType);
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    JSONObject beforeJson;
                    JSONObject afterJson;
                    if (eventType == CanalEntry.EventType.DELETE) {
                        beforeJson = column2json(rowData.getBeforeColumnsList());
                        afterJson = new JSONObject();
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        beforeJson = new JSONObject();
                        afterJson = column2json(rowData.getAfterColumnsList());
                    } else {
                        beforeJson = column2json(rowData.getBeforeColumnsList());
                        afterJson = column2json(rowData.getAfterColumnsList());
                    }
                    canalData.fluentPut("old", beforeJson).fluentPut("new", afterJson);
                    try {
                        log.warn("dispatch canal data to rocketmq, data:{}", canalData);
                        byte[] body = canalData.toString().getBytes(StandardCharsets.UTF_8);
                        Message message = new Message(canalDispatcherConfig.getTopic(), body);
                        DEFAULT_MQ_PRODUCER.send(message, MESSAGE_QUEUE_SELECTOR, canalDispatcherConfig.getHashKey());
                    } catch (Exception e) {
                        log.error("发送消息到rocketmq出错:{}", canalDispatcherConfig, e);
                    }
                }
            }
        }
    }

    private JSONObject column2json(List<CanalEntry.Column> columns) {
        JSONObject json = new JSONObject();
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        return json;
    }


    private CanalDispatcherConfig getCanalDispatcherConfig(String databaseName, String tableName) {
        try {
            List<CanalDispatcherConfig> canalDispatcherConfigs = Db.use().find(Entity.create().setTableName("canal_dispatcher_config")
                            .set("database_name", databaseName)
                            .set("table_name", tableName),
                    CanalDispatcherConfig.class);
            return CollUtil.getFirst(canalDispatcherConfigs);
        } catch (Exception ignored) {
        }
        return null;
    }


}
