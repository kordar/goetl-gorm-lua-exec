# 数据表
```sql
create table vd_report_etl_source
(
    id          bigint auto_increment
        primary key,
    name        varchar(128)            null comment '描述',
    `sql`       longtext                null comment '脚本',
    options     varchar(255) default '' null comment '执行参数',
    create_time datetime                not null comment '创建时间',
    update_time datetime                null comment '更新时间',
    deleted     tinyint      default 0  null comment '删除状态'
)
    comment 'ETL数据源';

create index vd_report_etl_source_name_index
    on vd_report_etl_source (name);

```

```sql
create table vd_report_etl_source_script
(
    id          bigint auto_increment
        primary key,
    sid         mediumtext                 not null comment '数据源ID',
    scripts     longtext                   not null comment '脚本',
    type        varchar(32)  default 'lua' null comment '类型',
    options     varchar(255) default ''    null comment '参数',
    create_time datetime                   not null comment '创建时间',
    update_time datetime                   not null comment '更新时间',
    deleted     tinyint      default 0     null comment '删除状态'
)
    comment '数据源脚本';

```

