package river

import (
	"bytes"
	"context"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

// masterInfo 负责 binlog 位点的持久化。
// 设计重点：
//  1. P1-6：原实现 1s 节流“丢弃 + 返回 nil”，反直觉且可能丢位点。现后台 goroutine 周期检查 dirty 标记，
//     取最新位点一次性落盘，调用方 Save 只更新内存 + 标 dirty，不会默默丢。
//  2. P1-7：atomic write 后强制 fsync 文件与父目录，保证断电后拿到的是最后一次成功写入的位点。
type masterInfo struct {
	sync.RWMutex

	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
	// GTID 已执行集合，COM_BINLOG_DUMP_GTID 续订依赖此字段；老 master.info 无此键时为空。
	GTID string `toml:"gtid_set"`

	filePath string

	// dirty 内存位点与磁盘不一致时为 true，后台 flush goroutine 扫这个标记决定是否写入。
	dirty bool

	// fsyncEnabled 是否在 rename 后强制 fsync，从配置透传
	fsyncEnabled bool

	// flush goroutine 生命周期控制
	flushInterval time.Duration
	cancel        context.CancelFunc
	done          chan struct{}
}

func loadMasterInfo(dataDir string, fsyncEnabled bool) (*masterInfo, error) {
	m := &masterInfo{
		fsyncEnabled:  fsyncEnabled,
		flushInterval: time.Second,
		done:          make(chan struct{}),
	}

	if len(dataDir) == 0 {
		return m, nil
	}

	m.filePath = path.Join(dataDir, "master.info")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return m, nil
	}
	defer f.Close()

	_, err = toml.DecodeReader(f, m)
	return m, errors.Trace(err)
}

// Run 启动后台 flush goroutine，周期检查 dirty 位并落盘。
// 调用方需 defer m.Stop() 以避免泄露。
func (m *masterInfo) Run() {
	if m.filePath == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go m.flushLoop(ctx)
}

// Stop 停止后台 goroutine，同时走一次同步 flush 以保证退出前最新位点落盘。
func (m *masterInfo) Stop() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
	// 透过最后一次持久化，避免 goroutine 退出后 dirty 还未落盘的场景
	if err := m.flushOnce(); err != nil {
		log.Errorf("masterInfo stop flush err: %v", err)
	}
}

func (m *masterInfo) flushLoop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.flushOnce(); err != nil {
				log.Errorf("masterInfo flush err: %v", err)
			}
		}
	}
}

// flushOnce 检查 dirty，如果有未落盘的修改则写入。是后台 goroutine 与 final flush 公用入口。
func (m *masterInfo) flushOnce() error {
	m.Lock()
	if !m.dirty || m.filePath == "" {
		m.Unlock()
		return nil
	}
	name, pos, gtid := m.Name, m.Pos, m.GTID
	m.Unlock()
	return m.persist(name, pos, gtid)
}

// Save 更新内存中的最新位点并标记 dirty，实际落盘交给 flushLoop。
// gtid 为空时保留已有 GTID，避免 canal 尚未吐出 GTID 事件时把已落盘集合冲掉。
func (m *masterInfo) Save(pos mysql.Position, gtid string) error {
	m.Lock()
	m.Name = pos.Name
	m.Pos = pos.Pos
	if gtid != "" {
		m.GTID = gtid
	}
	m.dirty = true
	m.Unlock()
	return nil
}

// SaveForce 立即同步落盘，不走 flushLoop。用于退出前的 final flush 场景。
func (m *masterInfo) SaveForce(pos mysql.Position, gtid string) error {
	m.Lock()
	m.Name = pos.Name
	m.Pos = pos.Pos
	if gtid != "" {
		m.GTID = gtid
	}
	name, p, g := m.Name, m.Pos, m.GTID
	m.dirty = true
	m.Unlock()
	return m.persist(name, p, g)
}

// persist 是唯一的落盘实现，使用临时文件 + rename + fsync 保证原子性与持久性。
func (m *masterInfo) persist(name string, pos uint32, gtid string) error {
	start := time.Now()
	defer func() {
		masterSaveDuration.Observe(time.Since(start).Seconds())
	}()

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(map[string]interface{}{
		"bin_name": name,
		"bin_pos":  pos,
		"gtid_set": gtid,
	}); err != nil {
		return errors.Trace(err)
	}

	dir := filepath.Dir(m.filePath)
	tmp, err := os.CreateTemp(dir, ".master.info.")
	if err != nil {
		return errors.Annotate(err, "create temp master.info")
	}
	tmpName := tmp.Name()
	// 失败路径上不能留下垃圾临时文件，使用 named return + defer cleanup
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = tmp.Write(buf.Bytes()); err != nil {
		tmp.Close()
		return errors.Annotate(err, "write temp master.info")
	}
	if m.fsyncEnabled {
		if err = tmp.Sync(); err != nil {
			tmp.Close()
			return errors.Annotate(err, "fsync temp master.info")
		}
	}
	if err = tmp.Close(); err != nil {
		return errors.Annotate(err, "close temp master.info")
	}

	if err = os.Rename(tmpName, m.filePath); err != nil {
		return errors.Annotate(err, "rename temp master.info")
	}
	committed = true

	if m.fsyncEnabled {
		// rename 后还要 fsync 父目录，保证目录项也落盘。Windows 打不开目录文件描述符，跳过即可。
		if df, derr := os.Open(dir); derr == nil {
			_ = df.Sync()
			_ = df.Close()
		}
	}

	m.Lock()
	m.dirty = false
	m.Unlock()

	log.Debugf("masterInfo persisted %s:%d gtid=%s", name, pos, gtid)
	return nil
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

// GTIDSet 返回已持久化的 GTID 集合字符串，空串表示尚未拿到 GTID。
func (m *masterInfo) GTIDSet() string {
	m.RLock()
	defer m.RUnlock()
	return m.GTID
}

func (m *masterInfo) Close() error {
	m.Stop()
	return nil
}
