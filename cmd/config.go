package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
}

type ManagerConfig struct {
	Host      *string  `yaml:"host"`
	Port      *int     `yaml:"port"`
	Workers   []string `yaml:"workers"`
	Scheduler *string  `yaml:"scheduler"`
	DBType    *string  `yaml:"dbtype"`

	DbTasksFile        *string `yaml:"db_tasks_file"`
	DbEventsFile       *string `yaml:"db_events_file"`
	DbTaskBucket       *string `yaml:"db_task_bucket"`
	DbEventBucket      *string `yaml:"db_event_bucket"`
	MaxRestarts        *int    `yaml:"max_restarts"`
	LoopInterval       *string `yaml:"loop_interval"`        // duration
	ApiShutdownTimeout *string `yaml:"api_shutdown_timeout"` // duration
}

type WorkerConfig struct {
	Host   *string `yaml:"host"`
	Port   *int    `yaml:"port"`
	Name   *string `yaml:"name"`
	DBType *string `yaml:"dbtype"`

	DbFilename         *string `yaml:"db_filename"`
	DbBucketName       *string `yaml:"db_bucket_name"`
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

// for cli's 'node', 'status', 'run', 'stop'
type ClientConfig struct {
	Manager *string `yaml:"manager"`
}

var (
	cfg        Config
	configPath string
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

	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("opening config directory %s: %w", dir, err)
	}
	defer func() {
		err := root.Close()
		if err != nil {
			log.Fatalln(err)
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
	case "node", "run", "status", "stop":
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
	case "node":
		return applyNodeRuntime(cfg.Node)
	default:
		return nil
	}
}

func applyManagerRuntime(m *ManagerConfig) error {
	if m == nil {
		return nil
	}
	if v := m.DbTasksFile; v != nil {
		if err := validateNonEmpty("manager.db_tasks_file", *v); err != nil {
			return err
		}
		manager.DbTasksFile = *v
	}
	if v := m.DbEventsFile; v != nil {
		if err := validateNonEmpty("manager.db_events_file", *v); err != nil {
			return err
		}
		manager.DbEventsFile = *v
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

// validators

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
