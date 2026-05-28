package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/sidchai/go-mysql-elasticsearch/river"
	"github.com/siddontang/go-log/log"
)

// 命令行 flag 覆盖 toml 配置，便于 systemd / k8s 推到同一镜像后只传凭证。
// 保留下划线命名以完全兼容原项目使用习惯。
var (
	configFile     = flag.String("config", "./etc/river.toml", "config file path")
	myAddr         = flag.String("my_addr", "", "MySQL addr (overrides toml)")
	myUser         = flag.String("my_user", "", "MySQL user (overrides toml)")
	myPass         = flag.String("my_pass", "", "MySQL password (overrides toml)")
	esAddr         = flag.String("es_addr", "", "Elasticsearch addr (overrides toml)")
	esUser         = flag.String("es_user", "", "Elasticsearch user (overrides toml)")
	esPass         = flag.String("es_pass", "", "Elasticsearch password (overrides toml)")
	esCAFile       = flag.String("es_ca_file", "", "Elasticsearch CA file path (overrides toml)")
	esInsecureSkip = flag.Bool("es_insecure_skip_tls", false, "skip TLS verification for ES (NOT recommended in prod)")
	dataDir        = flag.String("data_dir", "", "path to persist master.info")
	serverID       = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
	flavor         = flag.String("flavor", "", "flavor: mysql or mariadb")
	execution      = flag.String("exec", "", "mysqldump execution path")
	logLevel       = flag.String("log_level", "info", "log level")
)

func main() {
	// Go 1.5+ 默认 GOMAXPROCS=NumCPU，原代码重设其实是冗余；
	// 容器 cgroup 场景下可接入 uber-go/automaxprocs，不应该跳过。
	flag.Parse()

	log.SetLevelByName(*logLevel)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		log.Fatalf("load config %s err: %s", *configFile, errors.ErrorStack(err))
	}
	applyFlagOverrides(cfg)

	r, err := river.NewRiver(cfg)
	if err != nil {
		log.Fatalf("new river err: %s", errors.ErrorStack(err))
	}

	done := make(chan struct{}, 1)
	go func() {
		if err := r.Run(); err != nil {
			log.Errorf("river run err: %v", err)
		}
		done <- struct{}{}
	}()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	r.Close()
	<-done
}

// applyFlagOverrides 将命令行 flag 中非空的值写回配置。抽出函数便于单测与阅读。
func applyFlagOverrides(cfg *river.Config) {
	if *myAddr != "" {
		cfg.MyAddr = *myAddr
	}
	if *myUser != "" {
		cfg.MyUser = *myUser
	}
	if *myPass != "" {
		cfg.MyPassword = *myPass
	}
	if *esAddr != "" {
		cfg.ESAddr = *esAddr
	}
	if *esUser != "" {
		cfg.ESUser = *esUser
	}
	if *esPass != "" {
		cfg.ESPassword = *esPass
	}
	if *esCAFile != "" {
		cfg.ESCAFile = *esCAFile
	}
	if *esInsecureSkip {
		cfg.ESInsecureSkipTLS = true
	}
	if *serverID > 0 {
		cfg.ServerID = uint32(*serverID)
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *flavor != "" {
		cfg.Flavor = *flavor
	}
	if *execution != "" {
		cfg.DumpExec = *execution
	}
}
