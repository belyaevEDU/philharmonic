package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/manager"
	"github.com/spf13/cobra"
)

const defaultWorkerAddress = "localhost:5556"

func isDefaultWorkerList(workers []string) bool {
	return len(workers) == 1 && workers[0] == defaultWorkerAddress
}

var managerCmd = &cobra.Command{
	Use:   "manager",
	Short: "Manager command to launch a Philharmonic manager node",
	Long: `philharmonic manager command.

The manager controls the orchestration system. Is responsible for:
- Accepting tasks for users
- Scheduling tasks onto worker nodes
- Rescheduling tasks in the event of a node failure
- Periodically polling workers to get task updates`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			return err
		}
		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			return err
		}
		workers, err := cmd.Flags().GetStringSlice("workers")
		if err != nil {
			return err
		}
		scheduler, err := cmd.Flags().GetString("scheduler")
		if err != nil {
			return err
		}
		dbType, err := cmd.Flags().GetString("dbtype")
		if err != nil {
			return err
		}

		fmt.Println("Starting manager...")
		if isDefaultWorkerList(workers) {
			fmt.Printf("Notice: using the default worker list: %v\n", workers)
		}

		serverTLS, err := managerServerTLS()
		if err != nil {
			return err
		}
		workerClientTLS, err := managerWorkerClientTLS()
		if err != nil {
			return err
		}
		tokens, err := managerTokenStore()
		if err != nil {
			return err
		}

		// the manager talks to workers through the shared worker-facing client
		httpclient.ConfigureWorkerClient(httpclient.Options{TLSConfig: workerClientTLS})

		switch {
		case tokens == nil:
			fmt.Println("Warning: manager API auth is disabled (no manager.auth.token_file configured)")
		case serverTLS == nil:
			fmt.Println("Warning: bearer tokens will travel over plain HTTP (no manager.tls configured)")
		}

		m, err := manager.New(workers, scheduler, dbType)
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

		go m.ProcessTasks(ctx)
		go m.UpdateTasks(ctx)
		go m.DoHealthChecks(ctx)
		go m.RefreshNodeStats(ctx)

		api := manager.Api{Address: host, Port: port, Manager: m, TLSConfig: serverTLS, Auth: tokens}

		errCh := make(chan error, 1)
		go func() { errCh <- api.Start() }()

		select {
		case err := <-errCh:
			stop()
			runErr := err
			if serr := api.Shutdown(); serr != nil && runErr == nil {
				runErr = serr
			}
			if cerr := m.Close(); cerr != nil && runErr == nil {
				runErr = cerr
			}
			return runErr
		case <-ctx.Done():
		}
		stop()
		runErr := api.Shutdown()
		if cerr := m.Close(); cerr != nil && runErr == nil {
			runErr = cerr
		}
		return runErr
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)
	managerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	managerCmd.Flags().IntP("port", "p", 5555, "Port on which to listen")
	managerCmd.Flags().StringSliceP(
		"workers", "w", []string{defaultWorkerAddress},
		"List of addresses and ports of workers on which the manager will scheduler tasks",
	)
	managerCmd.Flags().StringP("scheduler", "s", "epvm", "Name of scheduler to use")
	managerCmd.Flags().StringP(
		"dbtype", "d", "memory",
		"Type of data storage to use for tasks (\"memory\" or \"bolt\")",
	)
}
