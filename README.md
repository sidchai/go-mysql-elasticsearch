# go-mysql-elasticsearch (sidchai fork)

> **Fork 来源**：[siddontang/go-mysql-elasticsearch](https://github.com/siddontang/go-mysql-elasticsearch)
> **维护者**：sidchai · 2026 年针对 saisiyun IoT 项目改造，适配 Elasticsearch 8/9 与 MySQL 8

[![ES](https://img.shields.io/badge/ES-8.x%20%7C%209.x-005571?logo=elasticsearch)](https://www.elastic.co/elasticsearch)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

将 MySQL binlog 实时同步到 Elasticsearch 的轻量级服务，相比 Canal Server + Adapter 方案：

- **资源**：单二进制 ~50MB，运行内存峰值 < 300MB（vs Canal 总计 4GB+）
- **依赖**：只需 Go 编译、mysqldump 二进制；无 JVM、无 Zookeeper
- **可观测**：内置 `/metrics` Prometheus 端点，暴露 8 个核心指标

## 为什么 Fork

上游项目 4 年未更新，仅支持 ES < 6.0、MySQL < 8.0。本 fork 主要改造：

| 维度 | 上游 | sidchai fork |
| --- | --- | --- |
| Elasticsearch | ≤ 5.x（含 `_type`） | 8.x / 9.x typeless API + `Accept: compatible-with=8` 头 |
| MySQL | ≤ 5.7 | 8.0+ 与 MariaDB 10.x（go-mysql v1.15.0） |
| Go | 1.12 | 1.25 |
| 依赖 | siddontang/go-mysql（已废弃） | go-mysql-org/go-mysql v1.15.0 |
| 错误处理 | bulk 失败被静默吞掉，位点照旧推进 | 重试 + 失败上抛，杜绝数据丢失（[P0-1](#bug-fixes)） |
| 网络 | 无超时、TLS 强制 InsecureSkipVerify | 可配 timeout + CA file + 严格 TLS（[P0-2/P0-4](#bug-fixes)） |
| 监控 | `canal_delay` 永远 0（goroutine 未启动） | 完整 8 项指标 + collectMetrics 真正运行（[P0-3](#bug-fixes)） |
| 位点持久化 | 1s 节流但可能丢位点 + 不 fsync | dirty mark 后台 flush + fsync 文件与父目录 |
| 健壮性 | OnRow 阻塞写 chan 卡死整个管道 | ctx select + 指数退避重试 + panic recover + final flush |
| 性能 | makeInsertReqData O(N×M) | 编译期映射 O(N) |
| 安全 | 凭证明文 toml | 支持 `${ENV_VAR}` 占位符 |

## Bug Fixes（重大）

完整审计报告见 [api_request_record_es_deployment.md](https://github.com/sidchai/iot_cloud_platform_server)，关键修复：

- **P0-1**：`doBulk` 错误判定写反 + 任意错误返回 nil → ES 5xx/4xx 被静默吞掉、位点照常推进、**数据永久丢失**。修复：仅 2xx & Errors=false 才认为成功，5xx 指数退避重试，错误强制上抛。
- **P0-2**：`http.Client` 无 Timeout → ES 故障时整个同步管道永久阻塞。修复：默认 30s 超时 + 连接池调优。
- **P0-3**：`collectMetrics` goroutine 从未启动 → `canal_delay` 监控永远 0。修复：`Run()` 中显式启动并接 ctx 退出。
- **P0-4**：HTTPS 强制 `InsecureSkipVerify=true` → 中间人攻击。修复：`es_ca_file` + `es_insecure_skip_tls` 配置项，默认严格校验。

## 快速开始

### 二进制（生产推荐）

```bash
# 1. 编译（Go 1.25+）
go build -trimpath -ldflags="-s -w" -o bin/go-mysql-es-iot ./cmd/go-mysql-elasticsearch

# 2. 配置（使用 ${ENV} 占位符）
cp etc/river.iot.toml /etc/go-mysql-es-iot/river.toml

# 3. 启动（凭证从环境变量注入）
MY_PASS=xxx ES_PASS=yyy ./bin/go-mysql-es-iot -config=/etc/go-mysql-es-iot/river.toml

# 4. 验证 metrics
curl -s http://127.0.0.1:12800/metrics | grep mysql2es_canal_state
# mysql2es_canal_state 1
```

### Docker Compose（开发/演示）

```bash
cp deploy/.env.example deploy/.env  # 填入 MY_PASS / ES_PASS
docker compose -f deploy/docker-compose.yml up -d

# 拉起本地 MySQL + ES（开发用）
docker compose -f deploy/docker-compose.yml --profile dev up -d
```

### Docker 单容器

```bash
docker run -d --name go-mysql-es-iot \
  -e MY_ADDR=db.internal:3306 -e MY_USER=canal -e MY_PASS=xxx \
  -e ES_ADDR=es.internal:9200 -e ES_USER=elastic -e ES_PASS=yyy \
  -p 127.0.0.1:12800:12800 \
  -v go-mysql-es-iot-data:/var/lib/go-mysql-es-iot \
  ghcr.io/sidchai/go-mysql-elasticsearch:latest
```

## 监控指标

| 指标 | 类型 | 说明 |
| --- | --- | --- |
| `mysql2es_canal_state` | Gauge | 同步状态 0/1 |
| `mysql2es_canal_delay` | Gauge | 主从延迟（秒） |
| `mysql2es_inserted_num{index}` | Counter | 累计写入文档数 |
| `mysql2es_updated_num{index}` | Counter | 累计更新文档数 |
| `mysql2es_deleted_num{index}` | Counter | 累计删除文档数 |
| `mysql2es_bulk_error_total{action,index,status}` | Counter | bulk 失败明细 |
| `mysql2es_bulk_retry_total` | Counter | bulk 整体重试次数 |
| `mysql2es_bulk_duration_seconds` | Histogram | bulk 耗时分布 |
| `mysql2es_sync_ch_size` | Gauge | 内部 chan 占用（背压） |
| `mysql2es_master_save_duration_seconds` | Histogram | master.info 落盘耗时 |

## 配置说明

完整模板见 [`etc/river.iot.toml`](./etc/river.iot.toml)，关键新增字段：

```toml
# ES TLS / 超时 / 连接池
es_ca_file = "/etc/ssl/es-ca.pem"
es_insecure_skip_tls = false
es_request_timeout = "30s"
es_max_idle_conns_per_host = 32

# bulk 重试（指数退避：1s -> 2s -> 4s -> ... -> 30s 封顶）
bulk_max_retry = 5
bulk_retry_initial_backoff = "1s"
bulk_retry_max_backoff = "30s"

# 内部 chan 容量
sync_ch_size = 4096

# master.info 强制 fsync（默认开启）
master_fsync = true
```

## 上游 README（保留参考）

> 以下为上游项目原文，部分版本约束（MySQL < 8.0、ES < 6.0）已被本 fork 突破。

---

## Call for Committer/Maintainer
Sorry that I have no enough time to maintain this project wholly, if you like this project and want to help me improve it continuously, please contact me through email (siddontang@gmail.com).

Requirement: In the email, you should list somethings(including but not limited to below) to make me believe we can work together.

Your GitHub ID
The contributions to go-mysql-elasticsearch before, including PRs or Issues.
The reason why you can improve go-mysql-elasticsearch.

## Install

+ Install Go (1.9+) and set your [GOPATH](https://golang.org/doc/code.html#GOPATH)
+ `go get github.com/siddontang/go-mysql-elasticsearch`, it will print some messages in console, skip it. :-)
+ cd `$GOPATH/src/github.com/siddontang/go-mysql-elasticsearch`
+ `make`

## How to use?

+ Create table in MySQL.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, Elasticsearch will create these automatically.
+ Config base, see the example config [river.toml](./etc/river.toml).
+ Set MySQL source in config file, see [Source](#source) below.
+ Customize MySQL and Elasticsearch mapping rule in config file, see [Rule](#rule) below.
+ Start `./bin/go-mysql-elasticsearch -config=./etc/river.toml` and enjoy it.

## Notice

+ MySQL supported version < 8.0
+ ES supported version < 6.0
+ binlog format must be **row**.
+ binlog row image must be **full** for MySQL, you may lost some field data if you update PK data in MySQL with minimal or noblob binlog row image. MariaDB only supports full row image.
+ Can not alter table format at runtime.
+ MySQL table which will be synced should have a PK(primary key), multi columns PK is allowed now, e,g, if the PKs is (a, b), we will use "a:b" as the key. The PK data will be used as "id" in Elasticsearch. And you can also config the id's constituent part with other column.
+ You should create the associated mappings in Elasticsearch first, I don't think using the default mapping is a wise decision, you must know how to search accurately.
+ `mysqldump` must exist in the same node with go-mysql-elasticsearch, if not, go-mysql-elasticsearch will try to sync binlog only.
+ Don't change too many rows at same time in one SQL.

## Source

In go-mysql-elasticsearch, you must decide which tables you want to sync into elasticsearch in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` includes the table need to be synced.

If you want to sync **all table in database**, you can use **asterisk(\*)**.  
```
[[source]]
schema = "test"
tables = ["*"]

# When using an asterisk, it is not allowed to sync multiple tables
# tables = ["*", "table"]
```

## Rule

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, use MySQL table field name as the Elasticserach's field name.  
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title,
the default field name is also named title.

Notice: go-mysql-elasticsearch will use the lower-case name for the ES index and type. E.g, if your table named BLOG, the ES index and type are both named blog.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"
id = ["id"]

    [rule.field]
    mysql = "title"
    elastic = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Rule field types

In order to map a mysql column on different elasticsearch types you can define the field type as follows:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"

    [rule.field]
    // This will map column title to elastic search my_title
    title="my_title"

    // This will map column title to elastic search my_title and use array type
    title="my_title,list"

    // This will map column title to elastic search title and use array type
    title=",list"

    // If the created_time field type is "int", and you want to convert it to "date" type in es, you can do it as below
    created_time=",date"
```

Modifier "list" will translates a mysql string field like "a,b,c" on an elastic array type '{"a", "b", "c"}' this is specially useful if you need to use those fields on filtering on elasticsearch.

## Wildcard table

go-mysql-elasticsearch only allows you determind which table to be synced, but sometimes, if you split a big table into multi sub tables, like 1024, table_0000, table_0001, ... table_1023, it is very hard to write rules for every table.

go-mysql-elasticserach supports using wildcard table, e.g:

```
[[source]]
schema = "test"
tables = ["test_river_[0-9]{4}"]

[[rule]]
schema = "test"
table = "test_river_[0-9]{4}"
index = "river"
type = "river"
```

"test_river_[0-9]{4}" is a wildcard table definition, which represents "test_river_0000" to "test_river_9999", at the same time, the table in the rule must be same as it.

At the above example, if you have 1024 sub tables, all tables will be synced into Elasticsearch with index "river" and type "river".

## Parent-Child Relationship

One-to-many join ( [parent-child relationship](https://www.elastic.co/guide/en/elasticsearch/guide/current/parent-child.html) in Elasticsearch ) is supported. Simply specify the field name for `parent` property.

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"
parent = "parent_id"
```

Note: you should [setup relationship](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-parent-field.html) with creating the mapping manually.

## Filter fields

You can use `filter` to sync specified fields, like:

```
[[rule]]
schema = "test"
table = "tfilter"
index = "test"
type = "tfilter"

# Only sync following columns
filter = ["id", "name"]
```

In the above example, we will only sync MySQL table tfiler's columns `id` and `name` to Elasticsearch. 

## Ignore table without a primary key
When you sync table without a primary key, you can see below error message.
```
schema.table must have a PK for a column
```
You can ignore these tables in the configuration like:
```
# Ignore table without a primary key
skip_no_pk_table = true
```

## Elasticsearch Pipeline
You can use [Ingest Node Pipeline](https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest.html) to pre-process documents before indexing, like JSON string decode, merge fileds and more.

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "_doc"

# pipeline id
pipeline = "my-pipeline-id"
```
Node: you should [create pipeline](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html) manually and Elasticsearch >= 5.0.

## Why not other rivers?

Although there are some other MySQL rivers for Elasticsearch, like [elasticsearch-river-jdbc](https://github.com/jprante/elasticsearch-river-jdbc), [elasticsearch-river-mysql](https://github.com/scharron/elasticsearch-river-mysql), I still want to build a new one with Go, why?

+ Customization, I want to decide which table to be synced, the associated index and type name, or even the field name in Elasticsearch.
+ Incremental update with binlog, and can resume from the last sync position when the service starts again.
+ A common sync framework not only for Elasticsearch but also for others, like memcached, redis, etc...
+ Wildcard tables support, we have many sub tables like table_0000 - table_1023, but want use a unique Elasticsearch index and type.

## Todo

+ MySQL 8
+ ES 6
+ Statistic.

## Donate

If you like the project and want to buy me a cola, you can through: 

|PayPal|微信|
|------|---|
|[![](https://www.paypalobjects.com/webstatic/paypalme/images/pp_logo_small.png)](https://paypal.me/siddontang)|[![](https://github.com/siddontang/blog/blob/master/donate/weixin.png)|

## Feedback

go-mysql-elasticsearch is still in development, and we will try to use it in production later. Any feedback is very welcome.

Email: siddontang@gmail.com
