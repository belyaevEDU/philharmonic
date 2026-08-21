package cmd

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/spf13/cobra"
)

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Fetch the logs of a task.",
	Long: `philharmonic logs command.

The logs command fetches the output (stdout+stderr) of a task from the
Philharmonic manager, which proxies the request to the worker running the
task's container. You may give either the task's UUID or the task's name.

Logs are captured by the worker when a container exits, is stopped, or is
declared unhealthy, so they survive container removal. While the container
is still running, logs are fetched live from Docker.

The task's state and (when known) the container exit code are printed as a
header line before the logs. Use --tail to limit the output to the last N
lines (passed through to Docker for live logs, applied to stored logs).`,

	SilenceUsage: true,

	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}
		tail, err := cmd.Flags().GetInt("tail")
		if err != nil {
			return err
		}

		encoded := url.PathEscape(args[0])
		requestURL := httpclient.ManagerURL(manager, "/tasks/logs/"+encoded)
		if tail > 0 {
			requestURL += fmt.Sprintf("?tail=%d", tail)
		}

		resp, err := httpclient.Manager().Get(requestURL) // #nosec G107
		if err != nil {
			return fmt.Errorf("error connecting to %s: %w", requestURL, err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error raised closing response body: %v\n", err)
			}
		}()

		if resp.StatusCode != http.StatusOK {
			errBody, _ := io.ReadAll(resp.Body)
			return fmt.Errorf(
				"error fetching logs: unexpected status %d from %s: %s",
				resp.StatusCode, requestURL, responseBodyMessage(errBody),
			)
		}

		out := cmd.OutOrStdout()

		// header line: state + exit code (if any)
		state := resp.Header.Get("X-Task-State")
		exitCode := resp.Header.Get("X-Exit-Code")
		if state == "" {
			state = "unknown"
		}
		if exitCode != "" {
			fmt.Fprintf(out, "state=%s exit=%s\n", state, exitCode)
		} else {
			fmt.Fprintf(out, "state=%s exit=-\n", state)
		}

		_, err = io.Copy(out, resp.Body)
		return err
	},
}

func init() {
	rootCmd.AddCommand(logsCmd)
	logsCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
	logsCmd.Flags().IntP("tail", "t", 0, "Number of lines to show from the end (0 = all)")
}
