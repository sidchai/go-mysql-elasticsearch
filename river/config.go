package river

import (
	"os"
	"regexp"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// SourceConfig is the configs for source
type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

// Config 是全局运行配置。新增字段默认值均在 applyDefaults 中补齐。
type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`
	MyCharset  string `toml:"my_charset"`

	ESHttps    bool   `toml:"es_https"`
	ESAddr     string `toml:"es_addr"`
	ESUser     string `toml:"es_user"`
	ESPassword string `toml:"es_pass"`

	// ESCAFile 可选 CA 证书路径，不为空时启用定制信任链
	ESCAFile string `toml:"es_ca_file"`
	// ESInsecureSkipTLS 默认 false（严格校验）；快速联调/自签证书可临时设 true
	ESInsecureSkipTLS bool `toml:"es_insecure_skip_tls"`
	// ESRequestTimeout 单次 HTTP 请求超时，默认 30s，避免 ES 挂死时永久阻塞
	ESRequestTimeout TomlDuration `toml:"es_request_timeout"`
	// ESMaxIdleConnsPerHost HTTP 连接池上限，默认 32；高 QPS 可调高
	ESMaxIdleConnsPerHost int `toml:"es_max_idle_conns_per_host"`

	StatAddr string `toml:"stat_addr"`
	StatPath string `toml:"stat_path"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec       string `toml:"mysqldump"`
	SkipMasterData bool   `toml:"skip_master_data"`

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime TomlDuration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`

	// BulkMaxRetry bulk 写入失败后的最大重试次数，0=不重试。默认 5。
	BulkMaxRetry int `toml:"bulk_max_retry"`
	// BulkRetryInitialBackoff 首次重试间隔，后续指数退避。默认 1s。
	BulkRetryInitialBackoff TomlDuration `toml:"bulk_retry_initial_backoff"`
	// BulkRetryMaxBackoff 重试最大退避上限。默认 30s。
	BulkRetryMaxBackoff TomlDuration `toml:"bulk_retry_max_backoff"`

	// SyncChSize 内部同步 chan 缓冲，默认 4096。背压圈变大可调高但会占内存。
	SyncChSize int `toml:"sync_ch_size"`

	// MasterFsync 默认 true；写入 master.info 后强制 fsync，避免断电丢位点。
	// 性能敏感场景可关闭，个人不推荐。
	MasterFsync *bool `toml:"master_fsync"`
}

// envVarPattern 匹配 ${VAR} 占位符（仅字母/数字/下划线），用于对 toml 里的凭证做环境变量替换。
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnvVars 在 toml 解析前将 ${VAR} 替换为环境变量值，未设置的变量保留原字面不报错。
// 这样生产可以把密码从 toml 中挑出，走 K8s Secret / systemd EnvironmentFile / docker -e 注入。
func expandEnvVars(data string) string {
	return envVarPattern.ReplaceAllStringFunc(data, func(match string) string {
		name := match[2 : len(match)-1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return match
	})
}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

// NewConfig creates a Config from data.
func NewConfig(data string) (*Config, error) {
	var c Config

	// 先展开环境变量占位符再走 toml 解析，避免密码明文走配置文件
	_, err := toml.Decode(expandEnvVars(data), &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.applyDefaults()
	return &c, nil
}

// applyDefaults 填充所有可选字段的默认值，保证老配置文件升级后不需动即可运行。
func (c *Config) applyDefaults() {
	if c.ESRequestTimeout.Duration == 0 {
		c.ESRequestTimeout.Duration = 30 * time.Second
	}
	if c.ESMaxIdleConnsPerHost == 0 {
		c.ESMaxIdleConnsPerHost = 32
	}
	if c.BulkMaxRetry == 0 {
		c.BulkMaxRetry = 5
	}
	if c.BulkRetryInitialBackoff.Duration == 0 {
		c.BulkRetryInitialBackoff.Duration = time.Second
	}
	if c.BulkRetryMaxBackoff.Duration == 0 {
		c.BulkRetryMaxBackoff.Duration = 30 * time.Second
	}
	if c.SyncChSize == 0 {
		c.SyncChSize = 4096
	}
}

// MasterFsyncEnabled 返回是否启用 fsync，默认 true。使用指针是为了区分“未设置”与“显式 false”。
func (c *Config) MasterFsyncEnabled() bool {
	if c.MasterFsync == nil {
		return true
	}
	return *c.MasterFsync
}

// TomlDuration supports time codec for TOML format.
type TomlDuration struct {
	time.Duration
}

// UnmarshalText implementes TOML UnmarshalText
func (d *TomlDuration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
