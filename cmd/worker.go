package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/belyaevedu/philharmonic/worker"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "worker command to operate a Cube worker node.",
	Long: `philharmonic worker command.

The worker runs tasks via Docker and responds to manager's requests about task states.`,
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
		name, err := cmd.Flags().GetString("name")
		if err != nil {
			return err
		}

		if name == "" {
			name = fmt.Sprintf("worker-%s", uuid.New())
		}
		dbType, err := cmd.Flags().GetString("dbtype")
		if err != nil {
			return err
		}

		log.Println("Starting worker...")

		log.Printf("Worker name: %s\n", name)
		w, err := worker.New(name, dbType)
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

		go w.RunTasks(ctx)
		go w.CollectStats(ctx)
		go w.UpdateTasks(ctx)

		api := worker.Api{Address: host, Port: port, Worker: w}

		errCh := make(chan error, 1)
		go func() { errCh <- api.Start() }()

		select {
		case err := <-errCh:
			stop()
			return err
		case <-ctx.Done():
		}
		stop()
		runErr := api.Shutdown()
		if cerr := w.Close(); cerr != nil && runErr == nil {
			runErr = cerr
		}
		return runErr
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 5556, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", "", "Name of the worker (auto-generated as worker-<uuid> when empty)")
	workerCmd.Flags().StringP(
		"dbtype", "d", "memory",
		"Type of data storage to use for tasks (\"memory\" or \"bolt\")",
	)
}
