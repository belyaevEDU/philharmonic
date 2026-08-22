package cmd

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/belyaevedu/philharmonic/auth"
	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/manager"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/scheduler"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/belyaevedu/philharmonic/worker"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// fields are pointers so that "key absent" (nil) can be told apart
// from "key present but zero value"

// slice fields stay nil when absent, which is enough to detect absence.

// Precedence for every setting is:
//
//	CLI flag (explicitly set)  >  Config file value  >  hardcoded default

type Config struct {
	Manager   *ManagerConfig   `yaml:"manager"`
	Worker    *WorkerConfig    `yaml:"worker"`
	Node      *NodeConfig      `yaml:"node"`
	Scheduler *SchedulerConfig `yaml:"scheduler"`
	Task      *TaskConfig      `yaml:"task"`
	Client    *ClientConfig    `yaml:"client"`
	HTTP      *HTTPConfig      `yaml:"http"`
}

type HTTPConfig struct {
	ClientTimeout *string `yaml:"client_timeout"` // duration

	// bounds long-running worker operations such as image pulls,
	// which routinely outlast client_timeout
	LongOpTimeout *string `yaml:"long_op_timeout"` // duration
}

// the shared shape of the per-role "tls" config sections
// which fields are meaningful depends on the role:
//
//	manager: cert/key = manager API server identity (also presented to
//	         workers when talking to them); ca_file = trust root for worker
//	         certificates; client_ca_file = require client certs (mTLS) on
//	         the manager API
//	worker:  cert/key = worker API server identity; client_ca_file =
//	         require client certs (mTLS), i.e. only certificate holders
//	         (normally the manager) may reach the worker API
//	client:  ca_file = trust root for the manager's (and workers')
//	         certificates; cert/key = client certificate presented
//	         to mTLS-protected servers
type TLSFileConfig struct {
	CertFile     *string `yaml:"cert_file"`
	KeyFile      *string `yaml:"key_file"`
	CAFile       *string `yaml:"ca_file"`
	ClientCAFile *string `yaml:"client_ca_file"`
}

// configures user authentication on the manager API
type ManagerAuthConfig struct {
	// TokenFile is a YAML file of bearer token records (user, role,
	// token_hash). Empty/nil disables authentication.
	TokenFile *string `yaml:"token_file"`
}

type ManagerConfig struct {
	Host      *string            `yaml:"host"`
	Port      *int               `yaml:"port"`
	Workers   []string           `yaml:"workers"`
	Scheduler *string            `yaml:"scheduler"`
	DBType    *string            `yaml:"dbtype"`
	TLS       *TLSFileConfig     `yaml:"tls"`
	Auth      *ManagerAuthConfig `yaml:"auth"`

	DbFile             *string `yaml:"db_file"`
	DbTaskBucket       *string `yaml:"db_task_bucket"`
	DbEventBucket      *string `yaml:"db_event_bucket"`
	MaxRestarts        *int    `yaml:"max_restarts"`
	LoopInterval       *string `yaml:"loop_interval"`        // duration
	ApiShutdownTimeout *string `yaml:"api_shutdown_timeout"` // duration
}

type WorkerConfig struct {
	Host   *string        `yaml:"host"`
	Port   *int           `yaml:"port"`
	Name   *string        `yaml:"name"`
	DBType *string        `yaml:"dbtype"`
	TLS    *TLSFileConfig `yaml:"tls"`

	DbFilename         *string `yaml:"db_filename"`
	DbBucketName       *string `yaml:"db_bucket_name"`
	DbLogBucketName    *string `yaml:"db_log_bucket_name"`
	LogCaptureMaxLines *int    `yaml:"log_capture_max_lines"`
	LoopInterval       *string `yaml:"loop_interval"`        // duration
	ApiShutdownTimeout *string `yaml:"api_shutdown_timeout"` // duration
}

type NodeConfig struct {
	StatsQueryMaxRetries  *int    `yaml:"stats_query_max_retries"`
	StatsQuerySleepPeriod *string `yaml:"stats_query_sleep_period"` // duration
	PortsQueryTimeout     *string `yaml:"ports_query_timeout"`      // duration
}

type SchedulerConfig struct {
	EpvmMaxJobs        *int     `yaml:"epvm_max_jobs"`
	EpvmMaxSafeCPUUtil *float64 `yaml:"epvm_max_safe_cpu_util"`
}

type TaskConfig struct {
	HealthCheckDefaultInterval    *int `yaml:"health_check_default_interval"`
	HealthCheckDefaultTimeout     *int `yaml:"health_check_default_timeout"`
	HealthCheckDefaultRetries     *int `yaml:"health_check_default_retries"`
	HealthCheckDefaultStartPeriod *int `yaml:"health_check_default_start_period"`
}

// for cli's 'nodes', 'status', 'run', 'stop'
type ClientConfig struct {
	Manager   *string        `yaml:"manager"`
	TLS       *TLSFileConfig `yaml:"tls"`
	Token     *string        `yaml:"token"`
	TokenFile *string        `yaml:"token_file"`
}

var (
	cfg        Config
	configPath string

	// configDir is the directory of the config file that was loaded (or, for a
	// missing file, where it would have been). Relative file paths in the
	// config (TLS certificates, token files) are resolved against it, so
	// "certs/manager.crt" in philharmonic.yaml means next to the config file
	// itself, regardless of the working directory the binary was started from.
	configDir string
)

func defaultConfigPath() string {
	exe, err := os.Executable()
	if err != nil {
		return "philharmonic.yaml"
	}
	return filepath.Join(filepath.Dir(exe), "philharmonic.yaml")
}

// a missing file is not an error: the caller falls back to the hardcoded
// defaults. Any other read or parse error is returned
func loadConfig(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolving config path %s: %w", path, err)
	}
	dir, file := filepath.Split(abs)
	configDir = dir

	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("opening config directory %s: %w", dir, err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			fmt.Printf("Error closing config root: %v\n", err)
		}
	}()

	data, err := root.ReadFile(file)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading config %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parsing config %s: %w", path, err)
	}
	return nil
}

// applyConfig overrides the resolved command's flag defaults with values from
// the loaded config. It only touches flags the user did not set on the
// command line, so explicit CLI flags always win.
// Must be called after cobra has parsed the flags,
// so that cmd.Flags().Changed reports accurately.
func applyConfig(cmd *cobra.Command) error {
	switch cmd.Name() {
	case "manager":
		if cfg.Manager != nil {
			if err := setStr(cmd, "host", cfg.Manager.Host); err != nil {
				return err
			}
			if err := setInt(cmd, "port", cfg.Manager.Port); err != nil {
				return err
			}
			if err := setSlice(cmd, "workers", cfg.Manager.Workers); err != nil {
				return err
			}
			if err := setStr(cmd, "scheduler", cfg.Manager.Scheduler); err != nil {
				return err
			}
			if err := setStr(cmd, "dbtype", cfg.Manager.DBType); err != nil {
				return err
			}
		}
	case "worker":
		if cfg.Worker != nil {
			if err := setStr(cmd, "host", cfg.Worker.Host); err != nil {
				return err
			}
			if err := setInt(cmd, "port", cfg.Worker.Port); err != nil {
				return err
			}
			if err := setStr(cmd, "name", cfg.Worker.Name); err != nil {
				return err
			}
			if err := setStr(cmd, "dbtype", cfg.Worker.DBType); err != nil {
				return err
			}
		}
	case "nodes", "run", "status", "stop", "logs":
		if cfg.Client != nil {
			if err := setStr(cmd, "manager", cfg.Client.Manager); err != nil {
				return err
			}
		}
	}
	return nil
}

func setStr(cmd *cobra.Command, name string, v *string) error {
	if v == nil || *v == "" || cmd.Flags().Changed(name) {
		return nil
	}
	return cmd.Flags().Set(name, *v)
}

func setInt(cmd *cobra.Command, name string, v *int) error {
	if v == nil || cmd.Flags().Changed(name) {
		return nil
	}
	return cmd.Flags().Set(name, strconv.Itoa(*v))
}

func setSlice(cmd *cobra.Command, name string, v []string) error {
	if len(v) == 0 || cmd.Flags().Changed(name) {
		return nil
	}
	// pflag's StringSlice parses CSV; addresses contain no commas, so a plain
	// comma-join round-trips correctly.
	return cmd.Flags().Set(name, strings.Join(v, ","))
}

// runtime tunable overrides (package vars <- config), with validation.
// must be called after loadConfig, from PersistentPreRunE
func applyRuntimeConfig(cmd *cobra.Command) error {
	if err := applyHTTPRuntime(cfg.HTTP); err != nil {
		return err
	}

	switch cmd.Name() {
	case "manager":
		if err := applyManagerRuntime(cfg.Manager); err != nil {
			return err
		}
		if err := applyNodeRuntime(cfg.Node); err != nil {
			return err
		}
		if err := applySchedulerRuntime(cfg.Scheduler); err != nil {
			return err
		}
		// the manager runs http/tcp health probes via task.Normalized
		return applyTaskRuntime(cfg.Task)
	case "worker":
		if err := applyWorkerRuntime(cfg.Worker); err != nil {
			return err
		}
		// the worker builds docker "exec" probes via task.Normalized
		return applyTaskRuntime(cfg.Task)
	case "nodes":
		if err := applyNodeRuntime(cfg.Node); err != nil {
			return err
		}
		return applyClientRuntime(cfg.Client)
	case "run", "status", "stop", "logs":
		return applyClientRuntime(cfg.Client)
	default:
		return nil
	}
}

func applyHTTPRuntime(h *HTTPConfig) error {
	if h == nil {
		return nil
	}
	if h.ClientTimeout != nil {
		d, err := parsePosDuration("http.client_timeout", *h.ClientTimeout)
		if err != nil {
			return err
		}
		httpclient.ClientTimeout = d
	}
	if h.LongOpTimeout != nil {
		d, err := parsePosDuration("http.long_op_timeout", *h.LongOpTimeout)
		if err != nil {
			return err
		}
		httpclient.LongOpTimeout = d
	}
	return nil
}

func applyManagerRuntime(m *ManagerConfig) error {
	if m == nil {
		return nil
	}
	if v := m.DbFile; v != nil {
		if err := validateNonEmpty("manager.db_file", *v); err != nil {
			return err
		}
		manager.DbFile = *v
	}
	if v := m.DbTaskBucket; v != nil {
		if err := validateNonEmpty("manager.db_task_bucket", *v); err != nil {
			return err
		}
		manager.DbTaskBucket = *v
	}
	if v := m.DbEventBucket; v != nil {
		if err := validateNonEmpty("manager.db_event_bucket", *v); err != nil {
			return err
		}
		manager.DbEventBucket = *v
	}
	if v := m.MaxRestarts; v != nil {
		if err := validateNonNegInt("manager.max_restarts", *v); err != nil {
			return err
		}
		manager.MaxRestarts = *v
	}
	if v := m.LoopInterval; v != nil {
		d, err := parsePosDuration("manager.loop_interval", *v)
		if err != nil {
			return err
		}
		manager.LoopInterval = d
	}
	if v := m.ApiShutdownTimeout; v != nil {
		d, err := parsePosDuration("manager.api_shutdown_timeout", *v)
		if err != nil {
			return err
		}
		manager.ApiShutdownTimeout = d
	}
	return nil
}

func applyWorkerRuntime(w *WorkerConfig) error {
	if w == nil {
		return nil
	}
	if v := w.DbFilename; v != nil {
		if err := validateDbFilename("worker.db_filename", *v); err != nil {
			return err
		}
		worker.DbFilename = *v
	}
	if v := w.DbBucketName; v != nil {
		if err := validateNonEmpty("worker.db_bucket_name", *v); err != nil {
			return err
		}
		worker.DbBucketName = *v
	}
	if v := w.DbLogBucketName; v != nil {
		if err := validateNonEmpty("worker.db_log_bucket_name", *v); err != nil {
			return err
		}
		worker.DbLogBucketName = *v
	}
	if v := w.LogCaptureMaxLines; v != nil {
		if err := validatePosInt("worker.log_capture_max_lines", *v); err != nil {
			return err
		}
		worker.LogCaptureMaxLines = *v
	}
	if v := w.LoopInterval; v != nil {
		d, err := parsePosDuration("worker.loop_interval", *v)
		if err != nil {
			return err
		}
		worker.LoopInterval = d
	}
	if v := w.ApiShutdownTimeout; v != nil {
		d, err := parsePosDuration("worker.api_shutdown_timeout", *v)
		if err != nil {
			return err
		}
		worker.ApiShutdownTimeout = d
	}
	return nil
}

func applyNodeRuntime(n *NodeConfig) error {
	if n == nil {
		return nil
	}
	if v := n.StatsQueryMaxRetries; v != nil {
		if err := validatePosInt("node.stats_query_max_retries", *v); err != nil {
			return err
		}
		node.StatsQueryMaxRetries = *v
	}
	if v := n.StatsQuerySleepPeriod; v != nil {
		d, err := parsePosDuration("node.stats_query_sleep_period", *v)
		if err != nil {
			return err
		}
		node.StatsQuerySleepPeriod = d
	}
	if v := n.PortsQueryTimeout; v != nil {
		d, err := parsePosDuration("node.ports_query_timeout", *v)
		if err != nil {
			return err
		}
		node.PortsQueryTimeout = d
	}
	return nil
}

func applySchedulerRuntime(s *SchedulerConfig) error {
	if s == nil {
		return nil
	}
	if v := s.EpvmMaxJobs; v != nil {
		if err := validatePosInt("scheduler.epvm_max_jobs", *v); err != nil {
			return err
		}
		scheduler.EpvmMaxJobs = *v
	}
	if v := s.EpvmMaxSafeCPUUtil; v != nil {
		if err := validateFraction("scheduler.epvm_max_safe_cpu_util", *v); err != nil {
			return err
		}
		scheduler.EpvmMaxSafeCPUUtil = *v
	}
	return nil
}

func applyTaskRuntime(t *TaskConfig) error {
	if t == nil {
		return nil
	}
	if v := t.HealthCheckDefaultInterval; v != nil {
		if err := validatePosInt("task.health_check_default_interval", *v); err != nil {
			return err
		}
		task.HealthCheckDefaultInterval = *v
	}
	if v := t.HealthCheckDefaultTimeout; v != nil {
		if err := validatePosInt("task.health_check_default_timeout", *v); err != nil {
			return err
		}
		task.HealthCheckDefaultTimeout = *v
	}
	if v := t.HealthCheckDefaultRetries; v != nil {
		if err := validatePosInt("task.health_check_default_retries", *v); err != nil {
			return err
		}
		task.HealthCheckDefaultRetries = *v
	}
	if v := t.HealthCheckDefaultStartPeriod; v != nil {
		if err := validateNonNegInt("task.health_check_default_start_period", *v); err != nil {
			return err
		}
		task.HealthCheckDefaultStartPeriod = *v
	}
	return nil
}

// builds the manager API server TLS config
// nil means plain HTTP
func managerServerTLS() (*tls.Config, error) {
	if cfg.Manager == nil || cfg.Manager.TLS == nil {
		return nil, nil
	}
	t := cfg.Manager.TLS
	c, err := auth.ServerTLSConfig(
		resolvePath(strVal(t.CertFile)),
		resolvePath(strVal(t.KeyFile)),
		resolvePath(strVal(t.ClientCAFile)),
	)
	if err != nil {
		return nil, fmt.Errorf("manager.tls: %w", err)
	}
	return c, nil
}

// builds the TLS config the manager uses as an HTTP client towards workers.
// the manager's own certificate pair is reused as the client certificate
// (a Philharmonic node certificate carries both serverAuth and clientAuth EKUs)
// nil means plain HTTP
func managerWorkerClientTLS() (*tls.Config, error) {
	if cfg.Manager == nil || cfg.Manager.TLS == nil {
		return nil, nil
	}
	t := cfg.Manager.TLS
	c, err := auth.ClientTLSConfig(
		resolvePath(strVal(t.CAFile)),
		resolvePath(strVal(t.CertFile)),
		resolvePath(strVal(t.KeyFile)),
	)
	if err != nil {
		return nil, fmt.Errorf("manager.tls: %w", err)
	}
	return c, nil
}

// builds the worker API server TLS config
// nil means plain HTTP
func workerServerTLS() (*tls.Config, error) {
	if cfg.Worker == nil || cfg.Worker.TLS == nil {
		return nil, nil
	}
	t := cfg.Worker.TLS
	c, err := auth.ServerTLSConfig(
		resolvePath(strVal(t.CertFile)),
		resolvePath(strVal(t.KeyFile)),
		resolvePath(strVal(t.ClientCAFile)),
	)
	if err != nil {
		return nil, fmt.Errorf("worker.tls: %w", err)
	}
	return c, nil
}

// loads the bearer-token store for the manager API
// nil means authentication is disabled
func managerTokenStore() (*auth.TokenStore, error) {
	if cfg.Manager == nil || cfg.Manager.Auth == nil {
		return nil, nil
	}
	path := resolvePath(strVal(cfg.Manager.Auth.TokenFile))
	if path == "" {
		return nil, nil
	}
	store, err := auth.LoadTokenFile(path)
	if err != nil {
		return nil, fmt.Errorf("manager.auth.token_file: %w", err)
	}
	return store, nil
}

// dereferences an optional config string ("" when absent)
func strVal(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// resolvePath anchors a possibly-relative file path from the config
// to the config file's own directory (configDir)
// absolute paths and empty strings pass through unchanged
func resolvePath(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(configDir, p)
}

// the environment variable the client commands read the manager API bearer token from.
// overrides client.token/token_file from the config file if present
const TokenEnvVar = "PHILHARMONIC_TOKEN"

// clientToken resolves the CLI bearer token:
// env PHILHARMONIC_TOKEN > client.token > client.token_file contents
func clientToken(c *ClientConfig) (string, error) {
	if token := os.Getenv(TokenEnvVar); token != "" {
		return token, nil
	}
	if c == nil {
		return "", nil
	}
	if token := strVal(c.Token); token != "" {
		if err := validateClientToken(token); err != nil {
			return "", err
		}
		return token, nil
	}
	if path := resolvePath(strVal(c.TokenFile)); path != "" { // #nosec G304, intended behaviour
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("client.token_file: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", fmt.Errorf("client.token_file: %s is empty", path)
		}
		if err := validateClientToken(token); err != nil {
			return "", fmt.Errorf("client.token_file: %w", err)
		}
		return token, nil
	}
	return "", nil
}

func applyClientRuntime(c *ClientConfig) error {
	token, err := clientToken(c)
	if err != nil {
		return err
	}

	var tlsCfg *tls.Config
	if c != nil && c.TLS != nil {
		tlsCfg, err = auth.ClientTLSConfig(
			resolvePath(strVal(c.TLS.CAFile)),
			resolvePath(strVal(c.TLS.CertFile)),
			resolvePath(strVal(c.TLS.KeyFile)),
		)
		if err != nil {
			return fmt.Errorf("client.tls: %w", err)
		}
	}

	httpclient.ConfigureManagerClient(httpclient.Options{TLSConfig: tlsCfg, BearerToken: token})
	httpclient.ConfigureWorkerClient(httpclient.Options{TLSConfig: tlsCfg})

	if token != "" && tlsCfg == nil {
		fmt.Printf("Warning: bearer token will be sent over plain HTTP (no client.tls configured)\n")
	}
	return nil
}

// validators

func validateClientToken(token string) error {
	if strings.HasPrefix(token, "sha256:") {
		return fmt.Errorf("configured token is a token_hash, not a token; put the raw token printed by \"philharmonic token\" (starts with \"ph_\") in client.token/token_file")
	}
	if strings.Contains(token, "- user:") {
		return fmt.Errorf("configured token looks like the YAML entry from the manager's token file; put only the raw token printed by \"philharmonic token\" (starts with \"ph_\") in client.token/token_file")
	}
	return nil
}

func cfgErr(field, msg string) error {
	return fmt.Errorf("config %s: %s", field, msg)
}

func validateNonEmpty(field, v string) error {
	if v == "" {
		return cfgErr(field, "must not be empty")
	}
	return nil
}

func validatePosInt(field string, v int) error {
	if v <= 0 {
		return cfgErr(field, fmt.Sprintf("must be > 0, got %d", v))
	}
	return nil
}

func validateNonNegInt(field string, v int) error {
	if v < 0 {
		return cfgErr(field, fmt.Sprintf("must be >= 0, got %d", v))
	}
	return nil
}

func validateFraction(field string, v float64) error {
	if v <= 0 || v > 1 {
		return cfgErr(field, fmt.Sprintf("must be in (0, 1], got %v", v))
	}
	return nil
}

// must contain a %s verb (the worker name is substituted into it) and must not
// introduce any other verbs, which would malformat the path
func validateDbFilename(field, v string) error {
	if v == "" {
		return cfgErr(field, "must not be empty")
	}
	if !strings.Contains(v, "%s") {
		return cfgErr(field, fmt.Sprintf("must contain a %%s verb for the worker name, got %q", v))
	}
	stripped := strings.ReplaceAll(v, "%s", "")
	if strings.Contains(stripped, "%") {
		return cfgErr(field, fmt.Sprintf("must contain only the %%s verb, got %q", v))
	}
	return nil
}

func parsePosDuration(field, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config %s: not a valid duration %q: %w", field, raw, err)
	}
	if d <= 0 {
		return 0, cfgErr(field, fmt.Sprintf("must be > 0, got %s", raw))
	}
	return d, nil
}
