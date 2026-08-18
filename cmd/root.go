package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "philharmonic",
	Short: "A container orchestration tool with manager/worker nodes",
	Long: `Philharmonic is a container orchestration tool with a manager/worker
architecture.

The manager accepts tasks from users, schedules them onto worker nodes,
monitors task health, and reschedules tasks in the event of a node failure.
Workers run tasks via Docker, collect resource stats, and report task state
back to the manager.

Run "philharmonic [command] --help" for details on a subcommand.

Configuration:
  An optional YAML config file supplies defaults for the manager and
  worker addresses/ports, the scheduler, the storage backend, and various
  runtime tunables (loop intervals, restart caps, health-check defaults,
  polling timeouts, ...).

  By default the config is read from philharmonic.yaml in the same
  directory as the philharmonic binary itself, so that an installed binary
  keeps its config next to it. Override the location with --config/-c; a
  missing file is silently ignored and the built-in defaults are used.

  Precedence for every setting:
    CLI flag (explicitly set)  >  config file value  >  built-in default`,

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := loadConfig(configPath); err != nil {
			return err
		}
		if err := applyConfig(cmd); err != nil {
			return err
		}
		return applyRuntimeConfig(cmd)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&configPath,
		"config", "c", defaultConfigPath(),
		"Path to the Philharmonic YAML config file (a missing file is silently ignored)",
	)
}
