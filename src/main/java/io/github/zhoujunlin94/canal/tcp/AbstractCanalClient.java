package io.github.zhoujunlin94.canal.tcp;

import cn.hutool.core.util.IdUtil;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import io.github.zhoujunlin94.canal.BaseCanalClient;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * @author zhoujunlin
 * @date 2024/7/14 11:00
 */
@Slf4j
public abstract class AbstractCanalClient extends BaseCanalClient {

    public AbstractCanalClient(String destination) {
        this(destination, null);
    }

    public AbstractCanalClient(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);

        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        running = true;
        thread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 1;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    MDC.put("requestId", IdUtil.fastSimpleUUID());
                    try {
                        // 获取指定数量的数据
                        Message message = connector.getWithoutAck(batchSize);
                        long batchId = message.getId();
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            try {
                                //log.warn("no canal data");
                                TimeUnit.MILLISECONDS.sleep(500);
                            } catch (InterruptedException ignored) {
                            }
                        } else {
                            handleSummary(message, batchId, size);
                            handleEntry(message.getEntries());
                        }
                        // 提交确认
                        connector.ack(batchId);
                    } finally {
                        MDC.remove("requestId");
                    }
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                log.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

}
