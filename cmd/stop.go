package cmd

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/belyaevedu/philharmonic/httpclient"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop a running task.",
	Long: `philharmonic stop command.

The stop command stops a running task.
You may give either the task's UUID or the task's name.`,

	SilenceUsage: true,

	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}

		// a name can contain characters that break an unescaped URL segment
		// (/, %, ?, spaces, ...); the manager enforces no charset on names at
		// submit time (Docker rejects bad ones later), so such tasks can exist.
		// PathEscape keeps a name from breaking or misrouting the request.
		encoded := url.PathEscape(args[0])
		requestURL := fmt.Sprintf("http://%s/tasks/%s", manager, encoded)
		client := httpclient.New()
		req, err := http.NewRequest(http.MethodDelete, requestURL, nil)
		if err != nil {
			return fmt.Errorf("error creating request %s: %w", requestURL, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error connecting to %s: %w", requestURL, err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error raised closing response body: %v\n", err)
			}
		}()

		if resp.StatusCode != http.StatusNoContent {
			errBody, _ := io.ReadAll(resp.Body)
			return fmt.Errorf(
				"error sending request: unexpected status %d from %s: %s",
				resp.StatusCode, requestURL, responseBodyMessage(errBody),
			)
		}

		fmt.Printf("Sent request to stop task %v.\n", args[0])
		return nil
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
	stopCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
}
