package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"text/tabwriter"

	"github.com/belyaevedu/philharmonic/node"
	"github.com/spf13/cobra"
)

const (
	memoryDivisor    = 1024.0
	nodeNotAvailable = "Not avail."
)

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Node command to list nodes.",
	Long: `philharmonic node command

The node command allows a user to get the information about the nodes in the cluster.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/nodes", manager)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				log.Fatalf("Error raised closing response body: %v\n", err)
			}
		}()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var nodes []*node.Node
		if err := json.Unmarshal(body, &nodes); err != nil {
			return fmt.Errorf("error unmarshalling nodes: %w", err)
		}

		var errs []error

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
		fmt.Fprintln(w, "NAME\tMEMORY (MiB)\tDISK (GiB)\tROLE\tnodes\t")
		for _, node := range nodes {
			stats, err := node.GetStats()
			if err != nil {
				fmt.Fprintf(
					w, "%s\t%s\t%s\t%s\t%s\t\n",
					node.Address,
					nodeNotAvailable, nodeNotAvailable,
					nodeNotAvailable, nodeNotAvailable,
				)
				errs = append(errs, err)
			}
			fmt.Fprintf(
				w, "%s\t%.2f\t%.2f\t%s\t%d\t\n",
				node.Address,
				float64(stats.MemTotalKb())/memoryDivisor, float64(stats.DiskTotal())/math.Pow(memoryDivisor, 3),
				node.Role, stats.TaskCount,
			)
		}

		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(nodeCmd)
	nodeCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
}
