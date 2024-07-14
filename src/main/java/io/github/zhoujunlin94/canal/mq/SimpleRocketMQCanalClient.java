package io.github.zhoujunlin94.canal.mq;

import cn.hutool.core.util.IdUtil;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoujunlin
 * @date 2024/7/14 17:04
 */
@Slf4j
public class SimpleRocketMQCanalClient extends AbstractRocektMQCanalClient {

    private final RocketMQCanalConnector connector;
    private static volatile boolean running = false;
    private Thread thread = null;

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> log.error("parse events has an error", e);

    public SimpleRocketMQCanalClient() {
        connector = new RocketMQCanalConnector(getRocketMQCanalNameServer(), getRocketMQCanalTopic(), getRocketMQCanalGroup(), false);
    }


    public static void main(String[] args) {
        try {
            SimpleRocketMQCanalClient rocketMQCanalClient = new SimpleRocketMQCanalClient();
            log.info("## Start the rocketmq consumer: {}-{}", rocketMQCanalClient.getRocketMQCanalTopic(), rocketMQCanalClient.getRocketMQCanalGroup());
            rocketMQCanalClient.start();
            log.info("## The canal rocketmq consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    log.info("## Stop the rocketmq consumer");
                    rocketMQCanalClient.stop();
                } catch (Throwable e) {
                    log.warn("## Something goes wrong when stopping rocketmq consumer:", e);
                } finally {
                    log.info("## Rocketmq consumer is down.");
                }
            }));
            //  不要停
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        } catch (Throwable e) {
            log.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        thread.start();
        running = true;
    }

    public void stop() {
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
    }

    private void process() {
        while (!running) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    // 获取message
                    List<Message> messages = connector.getListWithoutAck(500L, TimeUnit.MILLISECONDS);
                    for (Message message : messages) {
                        MDC.put("requestId", IdUtil.fastSimpleUUID());
                        try {
                            long batchId = message.getId();
                            int size = message.getEntries().size();
                            if (batchId == -1 || size == 0) {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(500);
                                } catch (InterruptedException ignored) {
                                }
                            } else {
                                handleSummary(message, batchId, size);
                                handleEntry(message.getEntries());
                            }
                        } finally {
                            MDC.remove("requestId");
                        }
                    }
                    // 提交确认
                    connector.ack();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        connector.unsubscribe();
    }

}
