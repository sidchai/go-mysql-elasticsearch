# =============================================================================
# go-mysql-elasticsearch (sidchai fork) 多阶段构建
#
# 设计要点：
#   - Stage 1: golang:1.25-alpine 拉依赖 + 静态编译（CGO_ENABLED=0），产物可任意基础镜像运行
#   - Stage 2: alpine:3.20 + tini(信号转发) + mariadb-client(提供 mysqldump 用于全量引导)
#   - 最终镜像 ~50MB（vs 单阶段 ~800MB），4G 机器友好
#   - 默认监听 12800/metrics，按 etc/river.iot.toml 配置
# =============================================================================

# ----- Stage 1: builder -----
FROM golang:1.25-alpine AS builder

# Alpine 编译依赖：git 拉私有模块、ca-certificates 验证 TLS
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# 分两步 COPY 利用 docker layer 缓存：依赖变更频率远低于业务代码
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# 静态编译：去掉 cgo + 去掉调试符号 + 关闭 timestamp 让镜像可重现构建
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /out/go-mysql-elasticsearch \
    ./cmd/go-mysql-elasticsearch

# ----- Stage 2: runtime -----
FROM alpine:3.20

LABEL maintainer="sidchai <sidchai@163.com>"
LABEL org.opencontainers.image.source="https://github.com/sidchai/go-mysql-elasticsearch"
LABEL org.opencontainers.image.description="MySQL binlog -> Elasticsearch 8/9 sync (forked from go-mysql-org)"

# tini：作为 PID 1 处理信号、回收僵尸进程；mariadb-client：提供 mysqldump 二进制做全量引导
# ca-certificates：访问启用 TLS 的 ES / MySQL 时需要
RUN apk add --no-cache tini mariadb-client ca-certificates tzdata && \
    addgroup -S app && adduser -S -G app app && \
    mkdir -p /var/lib/go-mysql-es-iot /etc/go-mysql-es-iot && \
    chown -R app:app /var/lib/go-mysql-es-iot /etc/go-mysql-es-iot

ENV TZ=Asia/Shanghai

COPY --from=builder /out/go-mysql-elasticsearch /usr/local/bin/go-mysql-elasticsearch
COPY --from=builder /src/etc/river.iot.toml /etc/go-mysql-es-iot/river.toml

USER app
WORKDIR /var/lib/go-mysql-es-iot

# 12800：Prometheus metrics 端点（与 river.iot.toml 中 stat_addr 一致）
EXPOSE 12800

# 健康检查：检查 mysql2es_canal_state 必须为 1（真实同步运行中）
# 原 grep -q mysql2es_canal_state 只判断指标存在，state=0（停止/异常）也算 healthy 是错的
# start-period 留 120s：全量阶段（mysqldump）期间 canal 还没消费 binlog，state=0 是正常的
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:12800/metrics 2>&1 | grep -E '^mysql2es_canal_state 1$' || exit 1

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/go-mysql-elasticsearch"]
CMD ["-config=/etc/go-mysql-es-iot/river.toml"]
