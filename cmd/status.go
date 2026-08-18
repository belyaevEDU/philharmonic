package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/docker/go-units"
	"github.com/spf13/cobra"
)

const (
	timeAgoFmt = "%s ago"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Status command to list tasks.",
	Long: `philharmonic status command.

The status command allows a user to get the status of tasks from the Philharmonic manager.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/tasks", manager)
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

		var tasks []*task.Task
		if err := json.Unmarshal(body, &tasks); err != nil {
			return fmt.Errorf("error unmarshalling tasks: %w", err)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', tabwriter.TabIndent)
		if _, err := fmt.Fprintln(w, "ID\tNAME\tAGE\tSTATE\tCONTAINERNAME\tIMAGE\t"); err != nil {
			return err
		}
		for _, task := range tasks {
			var start string
			if task.StartTime.IsZero() {
				start = fmt.Sprintf(timeAgoFmt, units.HumanDuration(time.Duration(0)))
			} else {
				start = fmt.Sprintf(timeAgoFmt, units.HumanDuration(time.Now().UTC().Sub(task.StartTime)))
			}

			state := task.State.String()
			if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t\n", task.ID, task.Name, start, state, task.Name, task.Image); err != nil {
				return err
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
}
