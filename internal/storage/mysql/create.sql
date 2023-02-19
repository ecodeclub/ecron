-- auto-generated definition
create table task_info
(
    id               int auto_increment
        primary key,
    name             varchar(128)                        null comment '任务名称',
    scheduler_status varchar(64)                         null comment '任务调度状态',
    cron             varchar(32)                         null comment '定时触发cron配置',
    type             varchar(32)                         null comment '任务类型',
    config           text                                null comment '执行配置',
    epoch            int       default 0                 null comment '调度状态改变轮次',
    create_time      timestamp default CURRENT_TIMESTAMP null,
    update_time      timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP
);

-- auto-generated definition
create table task_execution
(
    id             int auto_increment
        primary key,
    task_id        int                                 null comment '任务id',
    execute_status varchar(32)                         null comment '任务执行状态',
    create_time    timestamp default CURRENT_TIMESTAMP null,
    update_time    datetime  default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP
)
    comment '任务详情';

