create database if not exists `ecron`;

USE ecron

create table if not EXISTS `task_info`
(
    id                int auto_increment primary key,
    name              varchar(128)  not null comment '任务名称',
    type              varchar(32)   not null comment '任务类型',
    cron              varchar(32)   not null comment 'cron表达式',
    executor          varchar(128)  not null comment '执行器名称',
    version           int default 0 not null comment '用于实现乐观锁',
    status            int,
    cfg               text          not null comment '执行配置',
    next_exec_time    bigint comment '下一次执行时间',
    ctime       bigint        not null,
    utime       bigint        not null
    )
    comment '任务信息';

create table if not EXISTS `task_exec_history`
(
    id                int auto_increment primary key,
    tid               int not null,
    status            int,
    ctime       bigint        not null,
    utime       bigint        not null
    )
    comment '任务执行历史';

