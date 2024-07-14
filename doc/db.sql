CREATE DATABASE IF NOT EXISTS `common` CHARACTER SET 'utf8mb4';
USE `common`;

drop table IF EXISTS `canal_dispatcher_config`;
CREATE TABLE `canal_dispatcher_config`
(
    `id`            int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    `database_name` varchar(32)  NOT NULL DEFAULT '' COMMENT '数据库名',
    `table_name`    varchar(64)  NOT NULL DEFAULT '' COMMENT '表名',
    `topic`         varchar(32)  NOT NULL DEFAULT '' COMMENT '分发到那个topic',
    `hash_key`      varchar(32)  NOT NULL DEFAULT 'id' COMMENT '分区键',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_key` (`database_name`, `table_name`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  ROW_FORMAT = DYNAMIC COMMENT ='canal数据分发配置';
