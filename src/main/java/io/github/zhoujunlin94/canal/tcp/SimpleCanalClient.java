package io.github.zhoujunlin94.canal.tcp;

import cn.hutool.setting.Setting;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * @author zhoujunlin
 * @date 2024/7/14 11:29
 */
@Slf4j
public class SimpleCanalClient extends AbstractCanalClient {

    public SimpleCanalClient(String destination) {
        super(destination);
    }

    public static void main(String[] args) {
        // 根据ip，直接创建链接，无HA的功能
        Setting canalSetting = new Setting("canal.setting");
        String destination = canalSetting.get("canal.server.destination");
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalSetting.get("canal.server.ip"), canalSetting.getInt("canal.server.port")),
                destination, "", "");

        final SimpleCanalClient simpleCanalClient = new SimpleCanalClient(destination);
        simpleCanalClient.setConnector(connector);
        simpleCanalClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    log.info("## stop the canal client");
                    simpleCanalClient.stop();
                } catch (Throwable e) {
                    log.warn("##something goes wrong when stopping canal:", e);
                } finally {
                    log.info("## canal client is down.");
                }
            }

        });
    }

}
