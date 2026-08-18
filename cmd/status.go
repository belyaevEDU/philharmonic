package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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

The status command allows a user to get the status of tasks from the Philharmonic manager.

Running tasks with allocated host ports include a PORTS column, formatted like
docker ps. Use --filter Failed to focus on tasks that currently need attention.
In that mode, the final two columns show the current restart count and latest failure reason.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}
		filter, err := cmd.Flags().GetString("filter")
		if err != nil {
			return err
		}

		filter = strings.TrimSpace(filter)
		if filter != "" {
			valid := false
			for _, s := range knownStateNames() {
				if strings.EqualFold(s, filter) { // case-insensitive equal
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("invalid --filter %q: want one of %s",
					filter, strings.Join(knownStateNames(), ", "))
			}
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

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("manager returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var tasks []*task.Task
		if err := json.Unmarshal(body, &tasks); err != nil {
			return fmt.Errorf("error unmarshalling tasks: %w", err)
		}

		return writeTaskStatus(cmd.OutOrStdout(), tasks, filter)
	},
}

func writeTaskStatus(w io.Writer, tasks []*task.Task, filter string) error {
	filter = strings.ToLower(strings.TrimSpace(filter))
	showFailureDetails := filter == strings.ToLower(task.Failed.String())

	visibleTasks := make([]*task.Task, 0, len(tasks))
	showPorts := false
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if filter != "" && strings.ToLower(t.State.String()) != filter {
			continue
		}
		visibleTasks = append(visibleTasks, t)
		showPorts = showPorts || taskHasPorts(t)
	}

	tw := tabwriter.NewWriter(w, 0, 0, 5, ' ', tabwriter.TabIndent)
	header := "ID\tNAME\tAGE\tSTATE\tCONTAINERID\tIMAGE\t"
	if showPorts {
		header += "PORTS\t"
	}
	if showFailureDetails {
		header += "RESTARTS\tFAILURE\t"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}

	for _, t := range visibleTasks {
		var age string
		if t.StartTime.IsZero() {
			age = "-"
		} else {
			age = fmt.Sprintf(timeAgoFmt, units.HumanDuration(time.Now().UTC().Sub(t.StartTime)))
		}

		var err error
		switch {
		case showPorts && showFailureDetails:
			_, err = fmt.Fprintf(
				tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t\n",
				t.ID, t.Name, age, t.State.String(), statusContainerID(t), t.Image,
				taskPorts(t), t.RestartCount, statusFailure(t.FailureReason),
			)
		case showPorts:
			_, err = fmt.Fprintf(
				tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n",
				t.ID, t.Name, age, t.State.String(), statusContainerID(t), t.Image, taskPorts(t),
			)
		case showFailureDetails:
			_, err = fmt.Fprintf(
				tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t\n",
				t.ID, t.Name, age, t.State.String(), statusContainerID(t), t.Image,
				t.RestartCount, statusFailure(t.FailureReason),
			)
		default:
			_, err = fmt.Fprintf(
				tw, "%s\t%s\t%s\t%s\t%s\t%s\t\n",
				t.ID, t.Name, age, t.State.String(), statusContainerID(t), t.Image,
			)
		}
		if err != nil {
			return err
		}
	}

	return tw.Flush()
}

func taskHasPorts(t *task.Task) bool {
	if t == nil || t.State != task.Running {
		return false
	}
	for _, mapping := range t.HostPorts {
		if mapping.HostPort != 0 {
			return true
		}
	}
	return false
}

func taskPorts(t *task.Task) string {
	if !taskHasPorts(t) {
		return ""
	}

	parts := make([]string, 0, len(t.HostPorts))
	for _, mapping := range t.HostPorts {
		if mapping.HostPort == 0 {
			continue
		}

		protocol := strings.ToLower(string(mapping.Protocol))
		if protocol == "" {
			protocol = "tcp"
		}
		parts = append(parts, fmt.Sprintf("0.0.0.0:%d->%d/%s", mapping.HostPort, mapping.ContainerPort, protocol))
	}

	return strings.Join(parts, ", ")
}

func statusFailure(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "-"
	}
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(reason)
}

// like 'docker ps', we take the first 12 characters of the container id to be rendered
func statusContainerID(t *task.Task) string {
	id := strings.TrimSpace(t.ContainerID)
	if id == "" {
		return "-"
	}
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func knownStateNames() []string {
	return []string{
		task.Pending.String(),
		task.Scheduled.String(),
		task.Running.String(),
		task.Completed.String(),
		task.Failed.String(),
	}
}

func init() {
	rootCmd.AddCommand(statusCmd)
	statusCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
	statusCmd.Flags().StringP("filter", "f", "", "Filter tasks by state (one of Pending, Scheduled, Running, Completed, Failed)")
}
