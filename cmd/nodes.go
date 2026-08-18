package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Node command to list nodes.",
	Long: `philharmonic node command

The node command allows a user to get the information about the nodes in the cluster.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/nodes", manager)
		resp, err := http.Get(url) // #nosec G107
		if err != nil {
			return err
		}
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				fmt.Printf("Error raised closing response body: %v\n", err)
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
		if _, err := fmt.Fprintln(w, "NAME\tMEMORY (MiB)\tDISK (GiB)\tROLE\tTASKS\t"); err != nil {
			return err
		}
		for _, node := range nodes {
			stats, err := node.GetStats()
			if err != nil {
				if _, werr := fmt.Fprintf(
					w, "%s\t%s\t%s\t%s\t%s\t\n",
					node.Address,
					nodeNotAvailable, nodeNotAvailable,
					nodeNotAvailable, nodeNotAvailable,
				); werr != nil {
					errs = append(errs, werr)
				}
				errs = append(errs, err)
				continue
			}
			if _, werr := fmt.Fprintf(
				w, "%s\t%.2f\t%.2f\t%s\t%d\t\n",
				node.Address,
				float64(stats.MemTotalKb())/memoryDivisor,
				float64(stats.DiskTotal())/(memoryDivisor*memoryDivisor*memoryDivisor),
				node.Role, stats.TaskCount,
			); werr != nil {
				errs = append(errs, werr)
			}
		}
		if err := w.Flush(); err != nil {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			return errors.Join(errs...)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(nodesCmd)
	nodesCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
}
