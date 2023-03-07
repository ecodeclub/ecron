create table task_info
(
    id                int auto_increment
        primary key,
    name              varchar(128)  not null comment '任务名称',
    scheduler_status  varchar(64)   not null comment '任务调度状态',
    cron              varchar(32)   not null comment '定时触发cron配置',
    type              varchar(32)   not null comment '任务类型',
    config            text          not null comment '执行配置',
    epoch             int default 0 not null comment '调度状态改变轮次',
    occupier_id       int null comment '占有该任务的storage',
    occupier_payload  int  null comment '占有该任务的storage的载荷',
    candidate_id      int null comment '该任务的候选storage',
    candidate_payload int  null comment '该任务的候选storage的载荷',
    create_time       bigint        not null,
    update_time       bigint        not null
)
    comment '任务详情';

create index task_info_update_time_index
    on task_info (update_time);

create table task_execution
(
    id             int auto_increment
        primary key,
    task_id        int         not null comment '任务id',
    execute_status varchar(32) not null comment '任务类型',
    create_time    bigint      not null,
    update_time    bigint      not null
)
    comment '任务执行记录';

create index index_task_id
    on task_execution (id, task_id);

