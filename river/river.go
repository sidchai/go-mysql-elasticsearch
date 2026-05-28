package river

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/juju/errors"
	"github.com/sidchai/go-mysql-elasticsearch/elastic"
	"github.com/siddontang/go-log/log"
)

// ErrRuleNotExist is the error if rule is not defined.
var ErrRuleNotExist = errors.New("rule is not exist")

// River is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	es *elastic.Client

	master *masterInfo

	syncCh chan interface{}

	// runningOnce 保证 canalSyncState=1 只被设一次，避免重复获取锁 / 重复记录启动时间
	runningOnce sync.Once
}

// markRunning 首次被 OnRow 调用时设同步状态为 1，代表 binlog 端到端打通。
// 跳出 P1-3 “未连上却报 1”均有的问题，以 OnRow 为可靠序点。
func (r *River) markRunning() {
	r.runningOnce.Do(func() {
		canalSyncState.Set(1)
		log.Infof("river is running: state=1")
	})
}

// NewRiver creates the River from config
func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c
	r.rules = make(map[string]*Rule)
	// P2-3：从配置读 chan 容量，applyDefaults 已保证 >0
	r.syncCh = make(chan interface{}, c.SyncChSize)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	var err error
	// P1-7：透传 fsync 开关到 masterInfo
	if r.master, err = loadMasterInfo(c.DataDir, c.MasterFsyncEnabled()); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	// P0-2 / P0-4：透传 TLS/Timeout/连接池配置。NewClient 可能返回加载 CA 错误，必须向上传递。
	cfg := &elastic.ClientConfig{
		Addr:                r.c.ESAddr,
		User:                r.c.ESUser,
		Password:            r.c.ESPassword,
		HTTPS:               r.c.ESHttps,
		CAFile:              r.c.ESCAFile,
		InsecureSkipTLS:     r.c.ESInsecureSkipTLS,
		RequestTimeout:      r.c.ESRequestTimeout.Duration,
		MaxIdleConnsPerHost: r.c.ESMaxIdleConnsPerHost,
	}
	if r.es, err = elastic.NewClient(cfg); err != nil {
		return nil, errors.Annotate(err, "new es client")
	}

	// P1-2：InitStatus 返回 error，启动失败记 fatal 日志但不阻断主流程（同步仍可运行）
	go func() {
		if err := InitStatus(r.c.StatAddr, r.c.StatPath); err != nil {
			log.Errorf("metrics endpoint init failed: %v", err)
		}
	}()

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	cfg.Flavor = r.c.Flavor

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = r.c.SkipMasterData

	for _, s := range r.c.Sources {
		for _, t := range s.Tables {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+t)
		}
	}

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.Schema
		dbs[rule.Schema] = struct{}{}
		tables = append(tables, rule.Table)
	}

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}

func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}

	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) updateRule(schema, table string) error {
	rule, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := r.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	rule.TableInfo = tableInfo

	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		if !isValidTables(s.Tables) {
			return nil, errors.Errorf("wildcard * is not allowed for multiple tables")
		}

		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, buildTable(table), s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}

				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	if r.c.Rules != nil {
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.Schema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.Table) != rule.Table {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.Schema, rule.Table)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.Schema, rule.Table)
				}

				if len(rule.Index) == 0 {
					return errors.Errorf("wildcard table rule %s.%s must have a index, can not empty", rule.Schema, rule.Table)
				}

				rule.prepare()

				for _, table := range tables {
					rr := r.rules[ruleKey(rule.Schema, table)]
					rr.Index = rule.Index
					rr.Type = rule.Type
					rr.Parent = rule.Parent
					rr.ID = rule.ID
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.Schema, rule.Table)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.Schema, rule.Table)
				}
				rule.prepare()
				r.rules[key] = rule
			}
		}
	}

	rules := make(map[string]*Rule)
	for key, rule := range r.rules {
		if rule.TableInfo, err = r.canal.GetTable(rule.Schema, rule.Table); err != nil {
			return errors.Trace(err)
		}

		if len(rule.TableInfo.PKColumns) == 0 {
			if !r.c.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}

			log.Errorf("ignored table without a primary key: %s\n", rule.TableInfo.Name)
		} else {
			rules[key] = rule
		}
	}
	r.rules = rules

	return nil
}

func ruleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
}

// Run syncs the data from MySQL and inserts to ES.
// P1-3：启动阶段不设 canalSyncState=1，该状态由 markRunning（首次 OnRow 抵达）控制。
// P0-3：启动 collectMetrics goroutine 周期采样 canal_delay 与 syncCh 背压。
func (r *River) Run() error {
	r.wg.Add(1)
	go r.syncLoop()

	// P0-3：这个 goroutine 什么时候退出完全依赖 ctx，Close 中会 cancel
	go r.collectMetrics(r.ctx)

	// P1-6：启动 master.info 后台 flush goroutine
	r.master.Run()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		canalSyncState.Set(0)
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River.
// 退出顺序体现 graceful drain。
//  1. cancel：让 OnRow / syncLoop 能从 select 退出，避免动不了；
//  2. canal.Close：停止从 MySQL 拉取新的 binlog 事件；
//  3. wg.Wait：等 syncLoop 走完 final flush + final SaveForce 后退出；
//  4. master.Close：兼底再动一次 flushOnce，避免任何 dirty 未落盘。
func (r *River) Close() {
	log.Infof("closing river")

	r.cancel()
	r.canal.Close()
	r.wg.Wait()
	r.master.Close()
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}
