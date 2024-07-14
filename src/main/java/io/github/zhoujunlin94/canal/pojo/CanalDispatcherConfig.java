package io.github.zhoujunlin94.canal.pojo;

import lombok.Data;

/**
 * @author zhoujunlin
 * @date 2024/7/14 21:21
 */
@Data
public class CanalDispatcherConfig {

    private String databaseName;
    private String tableName;
    private String topic;
    private String hashKey;

}
