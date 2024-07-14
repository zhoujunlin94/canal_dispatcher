package io.github.zhoujunlin94.canal;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.SystemUtils;

import java.util.Date;
import java.util.List;

/**
 * @author zhoujunlin
 * @date 2024/7/14 16:22
 */
@Slf4j
public class BaseCanalClient {

    protected volatile boolean running = false;
    protected String destination;
    protected CanalConnector connector;
    protected Thread thread = null;
    protected Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> log.error("parse events has an error", e);

    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static String context_format;
    protected static String row_format;
    protected static String transaction_format;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memSize : [{}] , Time : {}" + SEP;
        context_format += "* StartPosition : [{}] " + SEP;
        context_format += "* EndPosition : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> row-binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;

        transaction_format = SEP
                + "================> {} transaction-binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;

    }

    protected void handleSummary(Message message, long batchId, int size) {
        printSummary(message, batchId, size);
    }

    private void printSummary(Message message, long batchId, int size) {
        long memSize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memSize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (CollUtil.isNotEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(CollUtil.getFirst(message.getEntries()));
            endPosition = buildPositionForDump(CollUtil.getLast(message.getEntries()));
        }
        log.info(context_format, batchId, size, memSize, DateUtil.now(), startPosition, endPosition);
    }

    private String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + DateUtil.formatDateTime(date) + ")";
        if (StrUtil.isNotBlank(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    protected void handleEntry(List<CanalEntry.Entry> entries) {
        printEntry(entries);
    }

    private void printEntry(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;
            Date executeDateTime = new Date(executeTime);
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry, e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    log.info(transaction_format, "begin thread id " + begin.getThreadId(),
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getExecuteTime(), DateUtil.formatDateTime(executeDateTime),
                            entry.getHeader().getGtid(), delayTime);
                    printXAInfo(begin.getPropsList());
                } else {
                    CanalEntry.TransactionEnd end;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry, e);
                    }
                    // 打印事务提交信息，事务id
                    log.info("----------------\n");
                    log.info(" END ----> transaction id: {}", end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    log.info(transaction_format, "end",
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getExecuteTime(), DateUtil.formatDateTime(executeDateTime),
                            entry.getHeader().getGtid(), delayTime);
                }
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChange;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry, e);
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                log.info(row_format,
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(),
                        entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType,
                        entry.getHeader().getExecuteTime(), DateUtil.formatDateTime(executeDateTime),
                        entry.getHeader().getGtid(), delayTime);

                if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                    log.info(" sql ----> " + rowChange.getSql() + SEP);
                    continue;
                }
                printXAInfo(rowChange.getPropsList());

                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList(), Lists.newArrayList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(Lists.newArrayList(), rowData.getAfterColumnsList());
                    } else {
                        printColumn(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    private void printXAInfo(List<CanalEntry.Pair> pairs) {
        if (pairs == null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for (CanalEntry.Pair pair : pairs) {
            String key = pair.getKey();
            if (StrUtil.endWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StrUtil.endWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            log.info(SEP + " XA_TYPE:{}, XA_XID:{} " + SEP, xaType, xaXid);
        }
    }

    private void printColumn(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> endColumns) {
        JSONObject beforeJson = new JSONObject();
        for (CanalEntry.Column column : beforeColumns) {
            beforeJson.put(column.getName(), column.getValue());
        }
        log.info("before:{}", beforeJson.toJSONString());

        JSONObject endJson = new JSONObject();
        for (CanalEntry.Column column : endColumns) {
            endJson.put(column.getName(), column.getValue());
        }
        log.info("end:{}", endJson.toJSONString());
    }


    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    /**
     * 获取当前Entry的 GTID信息示例
     *
     * @param header
     * @return
     */
    public static String getCurrentGTID(CanalEntry.Header header) {
        List<CanalEntry.Pair> props = header.getPropsList();
        if (CollUtil.isNotEmpty(props)) {
            for (CanalEntry.Pair pair : props) {
                if ("curtGtid".equals(pair.getKey())) {
                    return pair.getValue();
                }
            }
        }
        return "";
    }

    /**
     * 获取当前Entry的 GTID Sequence No信息示例
     *
     * @param header
     * @return
     */
    public static String getCurrentGTIDSeqNo(CanalEntry.Header header) {
        List<CanalEntry.Pair> props = header.getPropsList();
        if (CollUtil.isNotEmpty(props)) {
            for (CanalEntry.Pair pair : props) {
                if ("curtGtidSn".equals(pair.getKey())) {
                    return pair.getValue();
                }
            }
        }
        return "";
    }

    /**
     * 获取当前Entry的 GTID Last Committed信息示例
     *
     * @param header
     * @return
     */
    public static String getCurrentGTIDLastCommitted(CanalEntry.Header header) {
        List<CanalEntry.Pair> props = header.getPropsList();
        if (CollUtil.isNotEmpty(props)) {
            for (CanalEntry.Pair pair : props) {
                if ("curtGtidLct".equals(pair.getKey())) {
                    return pair.getValue();
                }
            }
        }
        return "";
    }

}
