-- auto-generated definition
create table task
(
    id               int auto_increment
        primary key,
    name             varchar(128) null comment '任务名称',
    scheduler_status varchar(64)  null comment '任务调度状态',
    execute_status   varchar(32)  null comment '任务执行状态',
    cron             varchar(32)  null comment '定时触发cron配置',
    type             varchar(32)  null comment '任务类型',
    config           text         null comment '执行配置'
);

