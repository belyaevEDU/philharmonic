package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/belyaevedu/philharmonic/auth"
	"github.com/spf13/cobra"
)

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Generate a manager API bearer token and its token-file entry",
	Long: `philharmonic token command.

Mints a random bearer token and prints it together with the YAML entry to
append to the manager's token file (manager.auth.token_file). Only the
SHA-256 hash of the token is stored; the token itself is shown once and
cannot be recovered later, so save it immediately.

Roles:
  admin: full access (run/stop/status/nodes);
  viewer: read-only access (status/nodes)`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		user, err := cmd.Flags().GetString("user")
		if err != nil {
			return err
		}
		role, err := cmd.Flags().GetString("role")
		if err != nil {
			return err
		}
		if strings.TrimSpace(user) == "" {
			return fmt.Errorf("--user must not be empty")
		}
		if role != string(auth.RoleAdmin) && role != string(auth.RoleViewer) {
			return fmt.Errorf("--role must be %q or %q, got %q", auth.RoleAdmin, auth.RoleViewer, role)
		}

		token, err := auth.GenerateToken()
		if err != nil {
			return err
		}

		// writes to a strings.Builder cannot fail, so the whole block is
		// assembled in memory and flushed to the real output with a single
		// checked write
		var b strings.Builder
		fmt.Fprintf(&b, "token: %s\n", token)
		fmt.Fprintf(&b, "\nAppend this entry to the manager's token file (manager.auth.token_file):\n")
		fmt.Fprintf(&b, "- user: %q\n", user)
		fmt.Fprintf(&b, "  role: %q\n", role)
		fmt.Fprintf(&b, "  token_hash: %q\n", auth.HashToken(token))
		fmt.Fprintf(&b, "\nTreat the token like a password; only its hash is stored on the manager.\n")

		if _, err := io.WriteString(cmd.OutOrStdout(), b.String()); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(tokenCmd)
	tokenCmd.Flags().StringP("user", "u", "", "Name of the user the token authenticates")
	tokenCmd.Flags().StringP("role", "r", string(auth.RoleAdmin), `Role of the user: "admin" or "viewer"`)
}
