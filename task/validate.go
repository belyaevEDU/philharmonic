package task

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/network"
)

// Validate collects every validation problem in the task's spec into one
// joined error, so a caller sees all problems at once rather than only the
// first. It returns nil for a valid spec.
//
// It covers everything the orchestrator checks before accepting or
// (re)starting a task: port mappings, restart policy, env, security
// options, and sanity of Timeout / MaxRestarts.
func (t Task) Validate() error {
	var errs []error

	if err := ValidatePortMappings(t.Ports); err != nil {
		errs = append(errs, err)
	}
	if err := ValidateRestartPolicy(t.RestartPolicy); err != nil {
		errs = append(errs, err)
	}
	if err := ValidateEnv(t.Env); err != nil {
		errs = append(errs, err)
	}
	if err := ValidateSecurity(t.Security); err != nil {
		errs = append(errs, err)
	}
	if t.Timeout < 0 {
		errs = append(errs, fmt.Errorf("task timeout must not be negative, got %d", t.Timeout))
	}
	if t.MaxRestarts < 0 {
		errs = append(errs, fmt.Errorf("task max_restarts must not be negative, got %d", t.MaxRestarts))
	}

	return errors.Join(errs...)
}

// accepts an empty policy, defaults to on-failure
func ValidateRestartPolicy(p string) error {
	switch p {
	case "", RestartPolicyNone, RestartPolicyAlways, RestartPolicyOnFailure, RestartPolicyUnlessStopped:
		return nil
	}
	return fmt.Errorf(
		"invalid restart policy %q: want %q, %q, %q, %q or empty",
		p, RestartPolicyNone, RestartPolicyAlways, RestartPolicyOnFailure, RestartPolicyUnlessStopped,
	)
}

// validates env entries in KEY=VALUE form
func ValidateEnv(env []string) error {
	for _, e := range env {
		key, _, found := strings.Cut(e, "=")
		if !found {
			return fmt.Errorf("invalid env entry %q: want KEY=VALUE", e)
		}
		if key == "" {
			return fmt.Errorf("invalid env entry %q: empty key", e)
		}
	}
	return nil
}

// validates a Security block; nil is valid ("no security options")
func ValidateSecurity(s *Security) error {
	if s == nil {
		return nil
	}

	if err := validateIDPair("user", s.User); err != nil {
		return err
	}
	for _, g := range s.GroupAdd {
		if err := validateIDPair("group_add entry", g); err != nil {
			return err
		}
	}

	for _, c := range s.CapAdd {
		if err := validateCapability(c); err != nil {
			return fmt.Errorf("invalid cap_add entry %q: %w", c, err)
		}
	}
	for _, c := range s.CapDrop {
		if err := validateCapability(c); err != nil {
			return fmt.Errorf("invalid cap_drop entry %q: %w", c, err)
		}
	}

	for _, t := range s.Tmpfs {
		path, _, _ := strings.Cut(t, ":")
		if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, " \t\n") {
			return fmt.Errorf("invalid tmpfs entry %q: want an absolute mount path, optionally followed by \":options\"", t)
		}
	}

	if s.PidsLimit < 0 {
		return fmt.Errorf("pids_limit must not be negative, got %d", s.PidsLimit)
	}

	for _, u := range s.Ulimits {
		if u.Name == "" {
			return errors.New("ulimit entry has an empty name")
		}
		if u.Soft < -1 || u.Hard < -1 {
			return fmt.Errorf(
				"ulimit entry %q: soft/hard must be -1 (unlimited) or non-negative, got soft=%d hard=%d",
				u.Name, u.Soft, u.Hard,
			)
		}
		if u.Soft > u.Hard {
			return fmt.Errorf("ulimit entry %q: soft limit %d exceeds hard limit %d", u.Name, u.Soft, u.Hard)
		}
	}

	switch s.IpcMode {
	case "", "private", "shareable", "none", "host":
	default:
		return fmt.Errorf("invalid ipc mode %q: want \"\", \"private\", \"shareable\", \"none\" or \"host\"", s.IpcMode)
	}

	switch {
	case s.UsernsMode == "", s.UsernsMode == "host":
	case strings.ContainsAny(s.UsernsMode, ": \t\n"):
		return fmt.Errorf("invalid userns_mode %q: want \"host\" or a daemon user namespace name", s.UsernsMode)
	}

	if s.Privileged && len(s.CapDrop) > 0 {
		return errors.New("privileged is mutually exclusive with cap_drop: a privileged container already holds every capability")
	}

	return nil
}

// validates "uid", "uid:gid", "user", "user:group" style specs:
// at most one colon, non-empty parts, no whitespace
func validateIDPair(what, spec string) error {
	if spec == "" {
		return nil
	}
	parts := strings.Split(spec, ":")
	if len(parts) > 2 {
		return fmt.Errorf("invalid %s %q: want \"uid\", \"uid:gid\", \"user\" or \"user:group\"", what, spec)
	}
	for _, p := range parts {
		if p == "" || strings.ContainsAny(p, " \t\n") {
			return fmt.Errorf("invalid %s %q: parts must be non-empty and contain no whitespace", what, spec)
		}
	}
	return nil
}

// validates a capability name: "ALL" or an optional CAP_ prefix followed by
// uppercase letters, digits and underscores; case-insensitive
func validateCapability(entry string) error {
	name := strings.ToUpper(entry)
	name = strings.TrimPrefix(name, "CAP_")
	if name == "" {
		return errors.New("empty capability name")
	}
	for _, r := range name {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return errors.New("want ALL or a capability like NET_ADMIN (with or without the CAP_ prefix)")
		}
	}
	return nil
}

// SCTP is intentionally unsupported, since the worker has no reliable SCTP inventory
func ValidatePortMappings(mappings []PortMapping) error {
	containerPorts := make(map[network.Port]struct{}, len(mappings))
	hostPorts := make(map[string]struct{}, len(mappings))

	for _, pm := range mappings {
		if pm.ContainerPort < 1 || pm.ContainerPort > 65535 {
			return fmt.Errorf("invalid container port %d in port mapping", pm.ContainerPort)
		}
		if pm.HostPort < 0 || pm.HostPort > 65535 {
			return fmt.Errorf("invalid host port %d in port mapping", pm.HostPort)
		}

		proto := network.TCP
		if pm.Protocol != "" {
			proto = pm.Protocol
		}
		if proto == network.SCTP {
			return fmt.Errorf("sctp port mappings are not supported")
		}
		if proto != network.TCP && proto != network.UDP {
			return fmt.Errorf("invalid protocol %s in port mapping (want tcp or udp)", pm.Protocol)
		}

		port, ok := network.PortFrom(clampToUint16(pm.ContainerPort), proto)
		if !ok {
			return fmt.Errorf("invalid port mapping %d/%s", pm.ContainerPort, proto)
		}
		if _, exists := containerPorts[port]; exists {
			return fmt.Errorf("duplicate container port mapping %d/%s", pm.ContainerPort, proto)
		}
		containerPorts[port] = struct{}{}

		if pm.HostPort != 0 {
			key := string(proto) + ":" + strconv.Itoa(pm.HostPort)
			if _, exists := hostPorts[key]; exists {
				return fmt.Errorf("duplicate host port mapping %d/%s", pm.HostPort, proto)
			}
			hostPorts[key] = struct{}{}
		}
	}
	return nil
}

// gosec G115 fix: preventing overflow
func clampToUint16(i int) uint16 {
	if i < 0 {
		return 0
	}
	if i > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(i)
}
