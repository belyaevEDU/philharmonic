package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/belyaevedu/philharmonic/worker"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "worker command to operate a Cube worker node.",
	Long: `philharmonic worker command.

The worker runs tasks via Docker and responds to manager's requests about task states.

When --name is empty the worker defaults to the host's hostname.
With --dbtype bolt the DB filename is derived from the name (<name>.db by default), so the name and the DB file are coupled.`,

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
			hostname, herr := os.Hostname()
			if herr != nil || strings.TrimSpace(hostname) == "" {
				hostname = uuid.NewString()
			}
			name = hostname
		}
		dbType, err := cmd.Flags().GetString("dbtype")
		if err != nil {
			return err
		}

		fmt.Println("Starting worker...")

		fmt.Printf("Worker name: %s\n", name)

		serverTLS, err := workerServerTLS()
		if err != nil {
			return err
		}
		if serverTLS != nil && serverTLS.ClientCAs == nil {
			fmt.Println("Notice: worker TLS is enabled without client_ca_file; " +
				"any client will be accepted. Set worker.tls.client_ca_file to require " +
				"manager certificates (mTLS).")
		}

		w, err := worker.New(name, dbType)
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

		go w.RunTasks(ctx)
		go w.CollectStats(ctx)
		go w.UpdateTasks(ctx)

		api := worker.Api{Address: host, Port: port, Worker: w, TLSConfig: serverTLS}

		errCh := make(chan error, 1)
		go func() { errCh <- api.Start() }()

		select {
		case err := <-errCh:
			stop()
			runErr := err
			if serr := api.Shutdown(); serr != nil && runErr == nil {
				runErr = serr
			}
			if cerr := w.Close(); cerr != nil && runErr == nil {
				runErr = cerr
			}
			return runErr
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
	workerCmd.Flags().StringP("name", "n", "", "Name of the worker (defaults to the host's hostname when empty)")
	workerCmd.Flags().StringP(
		"dbtype", "d", "memory",
		"Type of data storage to use for tasks (\"memory\" or \"bolt\")",
	)
}
