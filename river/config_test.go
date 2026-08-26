package river

import (
	"os"
	"testing"
	"time"
)

// TestExpandEnvVars 验证 P3-2 ${VAR} 占位符替换：
//   - 已设置环境变量 → 替换为环境变量值
//   - 未设置环境变量 → 保留原字面，不报错（避免锁死配置）
//   - 非占位符字符串 → 保持不变
func TestExpandEnvVars(t *testing.T) {
	t.Setenv("RIVER_TEST_PASS", "s3cr3t")

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"set", `pass = "${RIVER_TEST_PASS}"`, `pass = "s3cr3t"`},
		{"unset", `pass = "${RIVER_TEST_NOT_EXIST}"`, `pass = "${RIVER_TEST_NOT_EXIST}"`},
		{"plain", `addr = "127.0.0.1:3306"`, `addr = "127.0.0.1:3306"`},
		{"mixed", `url = "mysql://${RIVER_TEST_PASS}@host"`, `url = "mysql://s3cr3t@host"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expandEnvVars(tc.in)
			if got != tc.want {
				t.Errorf("expandEnvVars(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestApplyDefaults 验证默认值填充：零值 → 默认；非零值 → 不动
func TestApplyDefaults(t *testing.T) {
	c := &Config{}
	c.applyDefaults()

	if c.ESRequestTimeout.Duration != 30*time.Second {
		t.Errorf("default ESRequestTimeout = %v, want 30s", c.ESRequestTimeout.Duration)
	}
	if c.ESMaxIdleConnsPerHost != 32 {
		t.Errorf("default ESMaxIdleConnsPerHost = %d, want 32", c.ESMaxIdleConnsPerHost)
	}
	if c.BulkMaxRetry != 5 {
		t.Errorf("default BulkMaxRetry = %d, want 5", c.BulkMaxRetry)
	}
	if c.BulkRetryInitialBackoff.Duration != time.Second {
		t.Errorf("default BulkRetryInitialBackoff = %v, want 1s", c.BulkRetryInitialBackoff.Duration)
	}
	if c.BulkRetryMaxBackoff.Duration != 30*time.Second {
		t.Errorf("default BulkRetryMaxBackoff = %v, want 30s", c.BulkRetryMaxBackoff.Duration)
	}
	if c.SyncChSize != 4096 {
		t.Errorf("default SyncChSize = %d, want 4096", c.SyncChSize)
	}

	// 显式设置过的字段不应被覆盖
	c2 := &Config{
		ESMaxIdleConnsPerHost: 64,
		BulkMaxRetry:          10,
	}
	c2.applyDefaults()
	if c2.ESMaxIdleConnsPerHost != 64 {
		t.Errorf("overridden ESMaxIdleConnsPerHost = %d, want 64", c2.ESMaxIdleConnsPerHost)
	}
	if c2.BulkMaxRetry != 10 {
		t.Errorf("overridden BulkMaxRetry = %d, want 10", c2.BulkMaxRetry)
	}
}

// TestMasterFsyncEnabled 验证三态：未设置=true、显式 true=true、显式 false=false
func TestMasterFsyncEnabled(t *testing.T) {
	tt := true
	ff := false
	cases := []struct {
		name string
		ptr  *bool
		want bool
	}{
		{"nil_default_true", nil, true},
		{"explicit_true", &tt, true},
		{"explicit_false", &ff, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{MasterFsync: tc.ptr}
			if got := c.MasterFsyncEnabled(); got != tc.want {
				t.Errorf("MasterFsyncEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUseGTIDEnabled 验证三态：未设置=true（云库默认 GTID）、显式 true/false。
func TestUseGTIDEnabled(t *testing.T) {
	tt := true
	ff := false
	cases := []struct {
		name string
		ptr  *bool
		want bool
	}{
		{"nil_default_true", nil, true},
		{"explicit_true", &tt, true},
		{"explicit_false", &ff, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{UseGTID: tc.ptr}
			if got := c.UseGTIDEnabled(); got != tc.want {
				t.Errorf("UseGTIDEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestNewConfigUseGTIDToml 验证 toml 显式 false 能关掉 GTID 订阅。
func TestNewConfigUseGTIDToml(t *testing.T) {
	cfg, err := NewConfig(`
my_addr = "127.0.0.1:3306"
use_gtid = false
`)
	if err != nil {
		t.Fatalf("NewConfig err: %v", err)
	}
	if cfg.UseGTIDEnabled() {
		t.Errorf("use_gtid=false still enabled")
	}
}

// TestNewConfigEnvExpansion 端到端验证：toml 文本中的 ${VAR} 被解析前替换
func TestNewConfigEnvExpansion(t *testing.T) {
	t.Setenv("MY_TEST_PASS", "from_env")

	toml := `
my_addr = "127.0.0.1:3306"
my_user = "canal"
my_pass = "${MY_TEST_PASS}"
es_addr = "127.0.0.1:9200"
`
	cfg, err := NewConfig(toml)
	if err != nil {
		t.Fatalf("NewConfig err: %v", err)
	}
	if cfg.MyPassword != "from_env" {
		t.Errorf("MyPassword = %q, want from_env", cfg.MyPassword)
	}
	// applyDefaults 必须被调用
	if cfg.ESRequestTimeout.Duration == 0 {
		t.Errorf("applyDefaults not invoked: ESRequestTimeout=0")
	}
}

// TestNewConfigEnvUnset 验证：未设置的环境变量保留字面量，不影响解析
func TestNewConfigEnvUnset(t *testing.T) {
	if v, ok := os.LookupEnv("UNLIKELY_TO_EXIST_VAR_FOR_TEST"); ok {
		t.Skipf("env var unexpectedly set to %q, skipping", v)
	}

	toml := `my_pass = "${UNLIKELY_TO_EXIST_VAR_FOR_TEST}"`
	cfg, err := NewConfig(toml)
	if err != nil {
		t.Fatalf("NewConfig err: %v", err)
	}
	if cfg.MyPassword != "${UNLIKELY_TO_EXIST_VAR_FOR_TEST}" {
		t.Errorf("MyPassword = %q, want literal placeholder", cfg.MyPassword)
	}
}
