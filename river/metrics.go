package river

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siddontang/go-log/log"
)

var (
	// esInsertNum 累计写入 ES 的文档数（按 index 维度），衡量同步吞吐
	esInsertNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql2es_inserted_num",
			Help: "The number of docs inserted to elasticsearch",
		}, []string{"index"},
	)
	esUpdateNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql2es_updated_num",
			Help: "The number of docs updated to elasticsearch",
		}, []string{"index"},
	)
	esDeleteNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql2es_deleted_num",
			Help: "The number of docs deleted from elasticsearch",
		}, []string{"index"},
	)
	// canalSyncState 同步链路状态：0=未运行/异常停止，1=正常运行
	// 注意：仅当 canal binlog 真实连上并能消费事件后才设为 1（见 River.markRunning）
	canalSyncState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mysql2es_canal_state",
			Help: "The canal slave running state: 0=stopped, 1=ok",
		},
	)
	// canalDelay 主从延迟（秒），由 collectMetrics 周期采样
	canalDelay = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mysql2es_canal_delay",
			Help: "The canal slave lag in seconds",
		},
	)
	// esBulkErrorNum 失败的 bulk 单文档数量，按 action / index / status 维度，配合告警
	esBulkErrorNum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysql2es_bulk_error_total",
			Help: "The number of failed bulk items returned by Elasticsearch",
		}, []string{"action", "index", "status"},
	)
	// esBulkRetryNum bulk 整体重试次数，超过阈值说明 ES 处于不健康状态
	esBulkRetryNum = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "mysql2es_bulk_retry_total",
			Help: "The number of bulk-level retries triggered by transient errors",
		},
	)
	// esBulkDuration bulk 整体耗时分布（秒），定位慢请求
	esBulkDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "mysql2es_bulk_duration_seconds",
			Help:    "The duration of a bulk request to Elasticsearch",
			Buckets: []float64{0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5, 10, 30},
		},
	)
	// syncChSize 当前 syncCh 缓冲已用量，反映 ES 端写入慢导致的背压
	syncChSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "mysql2es_sync_ch_size",
			Help: "The current size of internal sync channel buffer",
		},
	)
	// masterSaveDuration master.info 持久化耗时（秒），异常变高说明磁盘 IO 异常
	masterSaveDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "mysql2es_master_save_duration_seconds",
			Help:    "The duration of persisting master.info to disk",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
	)
)

// collectMetrics 周期采样需要从 canal 获取的实时指标。
// 必须由 River.Run 显式启动并传入 ctx，关闭时立即退出。
func (r *River) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			canalDelay.Set(float64(r.canal.GetDelay()))
			syncChSize.Set(float64(len(r.syncCh)))
		}
	}
}

// InitStatus 启动 Prometheus metrics HTTP 服务。
// 使用独立 ServeMux 避免与全局 DefaultServeMux 冲突，确保多实例场景安全。
// 启动失败必须返回 error，由调用方决定是 fatal 还是降级。
func InitStatus(addr, path string) error {
	if addr == "" {
		log.Warnf("metrics addr is empty, /metrics endpoint disabled")
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Infof("start metrics server on %s%s", addr, path)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Errorf("metrics server exited: %v", err)
		return err
	}
	return nil
}
