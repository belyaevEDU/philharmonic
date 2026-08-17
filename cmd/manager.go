package cmd

import (
	"context"
	"log"

	"github.com/belyaevedu/philharmonic/manager"
	"github.com/spf13/cobra"
)

var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to launch a Philharmonic manager node",
	Long: `philharmonic manager command.

The manager controls the orchestration system. Is responsible for:
- Accepting tasks for users
- Scheduling tasks onto worker nodes
- Rescheduling tasks in the event of a node failure
- Periodically polling workers to get task updates`,
	Run: func(cmd *cobra.Command, args []string) {
		host, err := cmd.Flags().GetString("host")
		errOsExit(err) // I Wish There Was A Better Way To Do This...
		port, err := cmd.Flags().GetInt("port")
		errOsExit(err)
		workers, err := cmd.Flags().GetStringSlice("workers")
		errOsExit(err)
		scheduler, err := cmd.Flags().GetString("scheduler")
		errOsExit(err)
		dbType, err := cmd.Flags().GetString("dbtype")
		errOsExit(err)

		log.Println("Starting manager...")

		m, err := manager.New(workers, scheduler, dbType)
		errOsExit(err)

		ctx := context.Background()
		go m.ProcessTasks(ctx)
		go m.UpdateTasks(ctx)
		go m.DoHealthChecks(ctx)

		api := manager.Api{Address: host, Port: port, Manager: m}
		err = api.Start()
		errOsExit(err)
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)
	managerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	managerCmd.Flags().IntP("port", "p", 5555, "Port on which to listen")
	managerCmd.Flags().StringSliceP(
		"workers", "w", []string{"localhost:5556"},
		"List of addresses and ports of workers on which the manager will scheduler tasks",
	)
	managerCmd.Flags().StringP("scheduler", "s", "epvm", "Name of scheduler to use")
	managerCmd.Flags().StringP(
		"dbtype", "d", "memory",
		"Type of data storage to use for tasks (\"memory\" or \"boltdb\")",
	)
}
