package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"text/tabwriter"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/belyaevedu/philharmonic/node"
	"github.com/belyaevedu/philharmonic/stats"
	"github.com/spf13/cobra"
)

const (
	memoryDivisor    = 1024.0
	storageDivisor   = memoryDivisor * memoryDivisor * memoryDivisor
	nodeNotAvailable = "Not avail."
)

var nodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Node command to list nodes.",
	Long: `philharmonic nodes command

The nodes command allows a user to get information about the nodes in the cluster.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/nodes", manager)
		resp, err := httpclient.New().Get(url) // #nosec G107
		if err != nil {
			return fmt.Errorf("error connecting to manager: %w", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error raised closing response body: %v\n", err)
			}
		}()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("manager returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading manager response: %w", err)
		}

		var nodes []*node.Node
		if err := json.Unmarshal(body, &nodes); err != nil {
			return fmt.Errorf("error unmarshalling nodes: %w", err)
		}

		var errs []error

		w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 5, ' ', tabwriter.TabIndent)
		if _, err := fmt.Fprintln(w, "NAME\tMEMORY (MiB)\tMEMORY AVAILABLE (MiB)\tDISK (GiB)\tDISK AVAILABLE (GiB)\tCPU USAGE (%)\tROLE\tTASKS\t"); err != nil {
			return err
		}
		for _, n := range nodes {
			if n == nil {
				if werr := writeUnavailableNode(w, nodeNotAvailable); werr != nil {
					errs = append(errs, werr)
				}
				errs = append(errs, errors.New("manager returned a null node"))
				continue
			}

			stats, err := n.GetStats()
			if err != nil {
				if werr := writeUnavailableNode(w, n.Address); werr != nil {
					errs = append(errs, werr)
				}
				errs = append(errs, err)
				continue
			}
			if werr := writeNodeStats(w, n, stats); werr != nil {
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

func writeUnavailableNode(w io.Writer, address string) error {
	_, err := fmt.Fprintf(
		w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n",
		address,
		nodeNotAvailable, nodeNotAvailable, nodeNotAvailable, nodeNotAvailable,
		nodeNotAvailable, nodeNotAvailable, nodeNotAvailable,
	)
	return err
}

func writeNodeStats(w io.Writer, n *node.Node, s *stats.Stats) error {
	if n == nil {
		return errors.New("cannot write stats for a nil node")
	}
	if s == nil {
		writeErr := writeUnavailableNode(w, n.Address)
		return errors.Join(errors.New("node returned empty stats"), writeErr)
	}

	memoryTotal := s.MemTotalKb()
	memoryAvailable := s.MemAvailableKb()

	if memoryAvailable > memoryTotal {
		memoryAvailable = memoryTotal
	}

	diskTotal := s.DiskTotal()
	diskAvailable := s.DiskFree()
	if diskAvailable > diskTotal {
		diskAvailable = diskTotal
	}

	cpuUsage := nodeNotAvailable
	var metricErr error
	switch {
	case math.IsNaN(s.CpuUsage), math.IsInf(s.CpuUsage, 0), s.CpuUsage < 0, s.CpuUsage > 1:
		metricErr = fmt.Errorf("node %s returned invalid CPU usage %.4f", n.Address, s.CpuUsage)
	default:
		cpuUsage = fmt.Sprintf("%.2f", s.CpuUsage*100)
	}

	_, writeErr := fmt.Fprintf(
		w, "%s\t%.2f\t%.2f\t%.2f\t%.2f\t%s\t%s\t%d\t\n",
		n.Address,
		float64(memoryTotal)/memoryDivisor,
		float64(memoryAvailable)/memoryDivisor,
		float64(diskTotal)/storageDivisor,
		float64(diskAvailable)/storageDivisor,
		cpuUsage,
		n.Role, s.TaskCount,
	)
	return errors.Join(metricErr, writeErr)
}

func init() {
	rootCmd.AddCommand(nodesCmd)
	nodesCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
}
