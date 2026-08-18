package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a new task",
	Long: `philharmonic run command.

The run command starts a new task.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		manager, err := cmd.Flags().GetString("manager")
		if err != nil {
			return err
		}
		filename, err := cmd.Flags().GetString("filename")
		if err != nil {
			return err
		}

		fullFilePath, err := filepath.Abs(filename)
		if err != nil {
			return err
		}

		if !fileExists(fullFilePath) {
			return fmt.Errorf("file %s does not exist", filename)
		}

		fmt.Printf("Using manager: %v\n", manager)
		fmt.Printf("Using file: %v\n", fullFilePath)

		data, err := readFile(filename)
		if err != nil {
			return fmt.Errorf("unable to read file %s: %w", filename, err)
		}

		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data)) // #nosec G107
		if err != nil {
			return err
		}
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				fmt.Printf("Error raised closing response body: %v\n", err)
			}
		}()

		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("manager returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("reading manager response: %w", err)
		}

		var created task.Task
		if err := json.Unmarshal(body, &created); err != nil {
			return fmt.Errorf("decoding manager response: %w", err)
		}

		fmt.Printf("Successfully created task %q (id %s)\n", created.Name, created.ID)
		fmt.Printf("Stop it later with: philharmonic stop %s\n", created.Name)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
	runCmd.Flags().StringP("filename", "f", "task.json", "Task specification file (relative or absolute path)")
}

func readFile(filename string) ([]byte, error) {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := root.Close(); err != nil {
			fmt.Printf("Error closing root: %v\n", err)
		}
	}()

	return root.ReadFile(file)
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)

	return !errors.Is(err, fs.ErrNotExist)
}
