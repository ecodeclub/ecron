CREATE DATABASE IF NOT EXISTS `ecron` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci;
CREATE USER 'ecron'@'%' identified WITH mysql_native_password BY 'ecron1234';
GRANT CREATE,SELECT,INSERT,UPDATE ON ecron.* TO 'ecron'@'%';

USE `ecron`;

--
-- Table structure for table `node`
--
DROP TABLE IF EXISTS `node`;
CREATE TABLE `node` (
                        `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
                        `name` varchar(255) NOT NULL comment '节点名称',
                        `addr` varchar(256) NOT NULL comment '节点地址',
                        `online` varchar(256) NOT NULL comment '节点是否在线',
                        `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
                        `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
                        `deleted_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
                        PRIMARY KEY (`id`),
                        KEY `deleted_at` (`deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `task`
--
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
                        `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
                        `name` varchar(255) NOT NULL comment '任务名称',
                        `cron` varchar(256) NOT NULL comment '定时任务设定的执行时间',
                        `type` varchar(256) NOT NULL comment '任务的类型http_task',
                        `retry_count` tinyint NOT NULL comment '任务失败后的重试次数',
                        `retry_time` int(64) NOT NULL comment '任务失败后重试的时间',
                        `progress` varchar(256) NOT NULL comment '任务执行的进度',
                        `state` varchar(256) NOT NULL comment '任务的状态',
                        `node`  bigint(20) NOT NULL comment '任务被分配的节点的ID',
                        `task_job_type` tinyint NOT NULL comment '0：普通任务 1：定时任务',
                        `post_guid`  bigint(20) NOT NULL comment '任务创建时的全局唯一ID，防止重复插入任务',
                        `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
                        `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
                        `deleted_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
                        PRIMARY KEY (`id`),
                        UNIQUE KEY `name` (`name`),
                        UNIQUE KEY `post_guid` (`post_guid`),
                        KEY `state` (`state`),
                        KEY `task_job_type` (`task_job_type`),
                        KEY `deleted_at` (`deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
