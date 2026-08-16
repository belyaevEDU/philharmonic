package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/belyaevedu/philharmonic/worker"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "worker command to operate a Cube worker node.",
	Long: `philharmonic worker command.

The worker runs tasks via Docker and responds to manager's requests about task states.`,
	Run: func(cmd *cobra.Command, args []string) {
		host, err := cmd.Flags().GetString("host")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		name, err := cmd.Flags().GetString("name")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		dbType, err := cmd.Flags().GetString("dbtype")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		log.Println("Starting worker...")
		w, err := worker.New(name, dbType)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		ctx := context.Background()
		go w.RunTasks(ctx)
		go w.CollectStats(ctx)
		go w.UpdateTasks(ctx)

		api := worker.Api{Address: host, Port: port, Worker: w}
		if err := api.Start(); err != nil {
			log.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringP("host", "H", "0.0.0.0", "Hostname or IP address")
	workerCmd.Flags().IntP("port", "p", 5556, "Port on which to listen")
	workerCmd.Flags().StringP("name", "n", fmt.Sprintf("worker-%s", uuid.New().String()), "Name of the worker")
	workerCmd.Flags().StringP(
		"dbtype", "d", "memory",
		"Type of data storage to use for tasks (\"memory\" or \"boltdb\")",
	)
}
