package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a new task",
	Long: `philharmonic run command.

The run command starts a new task.`,
	SilenceUsage: true,
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

		log.Printf("Using manager: %v\n", manager)
		log.Printf("Using file: %v\n", fullFilePath)

		data, err := os.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("unable to read file %s: %w", filename, err)
		}

		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("manager returned status %d", resp.StatusCode)
		}

		log.Println("Successfully sent task request to manager")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP("manager", "m", "localhost:5555", "Manager to talk to")
	runCmd.Flags().StringP("filename", "f", "task.json", "Task specification file")
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)

	return !errors.Is(err, fs.ErrNotExist)
}
