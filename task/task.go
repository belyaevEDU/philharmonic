package task

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

type Action string
type Result string

const (
	ActionStart   Action = "start"
	ActionStop    Action = "stop"
	ResultSuccess Result = "success"
)

type PortMapping struct {
	ContainerPort int
	HostPort      int
	Protocol      network.IPProtocol
}

// restart policies a task can declare
const (
	RestartPolicyNone          = "no"
	RestartPolicyAlways        = "always"
	RestartPolicyOnFailure     = "on-failure"
	RestartPolicyUnlessStopped = "unless-stopped"
)

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

// reports whether a Failed task with this policy
// may be restarted by the orchestrator
func ShouldRestart(policy string) bool {
	switch policy {
	case "", RestartPolicyOnFailure, RestartPolicyAlways, RestartPolicyUnlessStopped:
		return true
	}
	return false
}

// reports whether a cleanly exited task
// with this policy may be restarted by the orchestrator
func ShouldRestartOnSuccess(policy string) bool {
	switch policy {
	case RestartPolicyAlways, RestartPolicyUnlessStopped:
		return true
	}
	return false
}

type HealthCheckType string

const (
	HealthCheckHTTP HealthCheckType = "http"
	HealthCheckTCP  HealthCheckType = "tcp"
	HealthCheckExec HealthCheckType = "exec"
)

var (
	HealthCheckDefaultInterval    = 30
	HealthCheckDefaultTimeout     = 5
	HealthCheckDefaultRetries     = 3
	HealthCheckDefaultStartPeriod = 0
)

type HealthCheck struct {
	Type        HealthCheckType
	Port        int
	Path        string
	Command     []string
	Interval    int
	Timeout     int
	Retries     int
	StartPeriod int
}

func (h *HealthCheck) Normalized() HealthCheck {
	n := HealthCheck{}
	if h != nil {
		n = *h
	}
	if n.Interval <= 0 {
		n.Interval = HealthCheckDefaultInterval
	}
	if n.Timeout <= 0 {
		n.Timeout = HealthCheckDefaultTimeout
	}
	if n.Retries <= 0 {
		n.Retries = HealthCheckDefaultRetries
	}
	if n.StartPeriod < 0 {
		n.StartPeriod = HealthCheckDefaultStartPeriod
	}
	return n
}

// mirrors compose's hardening-related options
type Security struct {
	// "uid", "uid:gid"
	User string

	// extra groups the container process belongs to
	GroupAdd []string

	// kernel capabilities
	CapAdd  []string
	CapDrop []string

	// mount an empty tmpfs at each path. "<path>:<options>"
	Tmpfs []string

	ReadOnlyRootfs  bool
	NoNewPrivileges bool
	Privileged      bool

	PidsLimit int64

	// per-resource rlimits for the container process, e.g. nofile
	Ulimits []Ulimit

	// "", "private", "shareable", "none" or "host"
	IpcMode string

	// re-map the container's UIDs through a user namespace
	UsernsMode string
}

// a single ulimit entry for Security.Ulimits
// Soft/Hard of -1 means "unlimited"
type Ulimit struct {
	Name string
	Soft int64
	Hard int64
}

type Task struct {
	ID            uuid.UUID
	ContainerID   string
	Name          string
	State         State
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Ports         []PortMapping
	Cmd           []string
	Env           []string
	RestartPolicy string
	HostPorts     []PortMapping // resolved bindings reported by the daemon

	HealthCheck     *HealthCheck
	Security        *Security `json:",omitempty"` // container hardening options
	RestartCount    int
	MaxRestarts     int    // per-task restart cap; 0 = fall back to manager.MaxRestarts
	FailureReason   string `json:",omitempty"`
	ManuallyStopped bool   `json:",omitempty"` // by user
	// earliest time the orchestrator will attempt the next restart
	// zero means immediately eligible
	NextRetryAt time.Time `json:",omitempty"`

	StartTime  time.Time
	FinishTime time.Time

	Timeout int64 // max seconds a task may run before the worker kills it, 0 = unlimited
}

func (t Task) Key() uuid.UUID { // store.Keyable impl
	return t.ID
}

func (t Task) EffectiveMaxRestarts(defaultMaxRestarts int) int {
	if t.MaxRestarts > 0 {
		return t.MaxRestarts
	}
	return defaultMaxRestarts
}

func (t Task) AtRestartCap(clusterMaxRestarts int) bool {
	return t.RestartCount >= t.EffectiveMaxRestarts(clusterMaxRestarts)
}

func (t Task) IsTerminal() bool {
	return t.State == Failed && !t.FinishTime.IsZero()
}

// captured snapshot of a task's container output,
// stored on the worker so logs survive container removal
type TaskLogs struct {
	TaskID     uuid.UUID
	ExitCode   *int   // nil if still running/unknown
	Logs       []byte // demultiplexed stdout+stderr, bounded at capture time
	CapturedAt time.Time
	Source     string // "exit" | "stop" | "unhealthy"
}

func (l TaskLogs) Key() uuid.UUID { // store.Keyable impl
	return l.TaskID
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

func (te TaskEvent) Key() uuid.UUID { // store.Keyable impl
	return te.ID
}

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	Ports         []PortMapping
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
	HealthCheck   *HealthCheck
	Security      *Security
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		Ports:         t.Ports,
		Cmd:           t.Cmd,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		Env:           t.Env,
		RestartPolicy: t.RestartPolicy,
		HealthCheck:   t.HealthCheck,
		Security:      t.Security,
	}
}

// host tmpfs mounts as the docker API wants them: path -> options.
// tmpfs options never contain colons, so the first ":" separates path from options
func (s *Security) dockerTmpfs() map[string]string {
	if s == nil || len(s.Tmpfs) == 0 {
		return nil
	}
	m := make(map[string]string, len(s.Tmpfs))
	for _, e := range s.Tmpfs {
		path, opts, _ := strings.Cut(e, ":")
		m[path] = opts
	}
	return m
}

// SecurityOpt entries for the docker API
func (s *Security) dockerSecurityOpts() []string {
	if s == nil {
		return nil
	}
	var opts []string
	if s.NoNewPrivileges {
		opts = append(opts, "no-new-privileges:true")
	}
	return opts
}

// ulimits as the docker API wants them
func (s *Security) dockerUlimits() []*container.Ulimit {
	if s == nil || len(s.Ulimits) == 0 {
		return nil
	}
	out := make([]*container.Ulimit, 0, len(s.Ulimits))
	for _, u := range s.Ulimits {
		out = append(out, &container.Ulimit{Name: u.Name, Soft: u.Soft, Hard: u.Hard})
	}
	return out
}

// nil unless a positive PIDs limit was requested
func (s *Security) dockerPidsLimit() *int64 {
	if s == nil || s.PidsLimit <= 0 {
		return nil
	}
	limit := s.PidsLimit
	return &limit
}

func (c *Config) dockerPorts() (network.PortSet, network.PortMap, error) {
	if err := ValidatePortMappings(c.Ports); err != nil {
		return nil, nil, err
	}

	exposed := network.PortSet{}
	bindings := network.PortMap{}

	for _, pm := range c.Ports {
		proto := network.TCP
		if pm.Protocol != "" {
			proto = pm.Protocol
		}

		port, _ := network.PortFrom(clampToUint16(pm.ContainerPort), proto)
		exposed[port] = struct{}{}

		hostPort := ""
		if pm.HostPort != 0 {
			hostPort = strconv.Itoa(pm.HostPort)
		}
		bindings[port] = []network.PortBinding{{HostPort: hostPort}}
	}

	return exposed, bindings, nil
}

func PortMappingsFromPortMap(ports network.PortMap) []PortMapping {
	var out []PortMapping

	for p, bindings := range ports {
		for _, b := range bindings {
			hostPort, err := strconv.Atoi(b.HostPort)
			if err != nil {
				continue
			}

			out = append(out, PortMapping{
				ContainerPort: int(p.Num()),
				HostPort:      hostPort,
				Protocol:      p.Proto(),
			})
		}
	}

	// just for the api to be deterministic
	// because go maps' iteration order is basically randomized
	slices.SortFunc(out, func(a, b PortMapping) int {
		return cmp.Or(
			cmp.Compare(a.ContainerPort, b.ContainerPort),
			cmp.Compare(a.HostPort, b.HostPort),
			cmp.Compare(a.Protocol, b.Protocol),
		)
	})

	// the daemon reports both IPv4 and IPv6 for every binding, so
	out = slices.CompactFunc(out, func(a, b PortMapping) bool {
		return a == b
	})

	return out
}

// "exec" probes are managed by Docker, "http"/"tcp" are done by the manager
func (c *Config) dockerHealthcheck() *container.HealthConfig {
	hc := c.HealthCheck
	if hc == nil || hc.Type != HealthCheckExec || len(hc.Command) == 0 {
		return nil
	}

	n := hc.Normalized()
	return &container.HealthConfig{
		Test:        append([]string{"CMD"}, n.Command...),
		Interval:    time.Duration(n.Interval) * time.Second,
		Timeout:     time.Duration(n.Timeout) * time.Second,
		StartPeriod: time.Duration(n.StartPeriod) * time.Second,
		Retries:     n.Retries,
	}
}

type Docker struct {
	Client *client.Client
	Config Config
}

type DockerInspectResponse struct {
	Response *container.InspectResponse
	Error    error
}

func (d *Docker) Inspect(containerID string) DockerInspectResponse {
	if d.Client == nil {
		return DockerInspectResponse{
			Error: errors.New("error inspecting a container: Docker.Client is nil"),
		}
	}

	ctx := context.Background()

	// Size controls whether the container's filesystem size should be calculated
	cio := client.ContainerInspectOptions{Size: true}
	resp, err := d.Client.ContainerInspect(ctx, containerID, cio)
	if err != nil {
		log.Printf("Error inspecting container: %v\n", err)
		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Response: &resp.Container}
}

// makes sure image exists on the local daemon,
// pulling it from the registry only when missing
//
// it reports whether the image had to be pulled
//
// the pull's progress stream is drained rather than printed,
// callers get a plain success/error
func (d *Docker) Pull(image string) (bool, error) {
	ctx := context.Background()

	if _, err := d.Client.ImageInspect(ctx, image); err == nil {
		log.Printf("Image %s already exists locally; skipping pull\n", image)
		return false, nil
	} else if !errdefs.IsNotFound(err) {
		log.Printf("Error inspecting image %s: %v\n", image, err)
		return false, fmt.Errorf("inspecting image %s: %w", image, err)
	}

	log.Printf("Image %s not found locally; pulling\n", image)
	reader, err := d.Client.ImagePull(ctx, image, client.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", image, err)
		return false, fmt.Errorf("pulling image %s: %w", image, err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing the image pull progress stream for %s: %v\n", image, err)
		}
	}()

	// the pull isn't complete until EOF
	if _, err := io.Copy(io.Discard, reader); err != nil {
		log.Printf(
			"Error reading the pull progress stream for image %s: %v\n",
			image, err,
		)
		return false, fmt.Errorf("reading pull progress for image %s: %w", image, err)
	}

	log.Printf("Pulled image %s\n", image)
	return true, nil
}

// fetches the demultiplexed stdout+stderr of a container into a single buffer
// a missing container yields (nil, nil) so callers can fall back to stored logs
// without treating that as an error
func (d *Docker) Logs(containerID string, tail int) ([]byte, error) {
	if d.Client == nil {
		return nil, errors.New("error fetching logs: Docker.Client is nil")
	}
	if containerID == "" {
		return nil, nil
	}

	opts := client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}
	if tail > 0 {
		opts.Tail = strconv.Itoa(tail)
	}

	ctx := context.Background()
	out, err := d.Client.ContainerLogs(ctx, containerID, opts)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("error fetching logs for container %s: %w", containerID, err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Printf("Error closing logs for container %s: %v\n", containerID, err)
		}
	}()

	var buf bytes.Buffer
	if _, err := stdcopy.StdCopy(&buf, &buf, out); err != nil {
		return nil, fmt.Errorf("error demultiplexing logs for container %s: %w", containerID, err)
	}
	return buf.Bytes(), nil
}

func NewDocker(c *Config) (*Docker, error) {
	dc, err := client.New(client.FromEnv)
	if err != nil {
		// not capitalized because linter yelled at me
		// also if its going to outputted its going to be wrapping in an other message
		return nil, fmt.Errorf("error creating a docker instance: %v", err)
	}

	return &Docker{
		Client: dc,
		Config: *c,
	}, nil
}

type DockerResult struct {
	ContainerID string
	Action      Action
	Result      Result
	ExitCode    *int // container exit code, when known
	Error       error
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()

	// pull only when the image isn't already present on this worker
	if _, err := d.Pull(d.Config.Image); err != nil {
		return DockerResult{Error: err}
	}

	// the orchestrator manages the restart policy itself
	rp := container.RestartPolicy{
		Name: container.RestartPolicyDisabled,
	}

	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	exposed, bindings, err := d.Config.dockerPorts()
	if err != nil {
		log.Printf("Error building port config for image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Cmd:          d.Config.Cmd,
		Env:          d.Config.Env,
		ExposedPorts: exposed,
		Healthcheck:  d.Config.dockerHealthcheck(),
	}

	hc := container.HostConfig{
		RestartPolicy: rp,
		Resources:     r,
		PortBindings:  bindings,
	}

	// security/hardening options. a nil Security means "no options" -> runs with daemon defaults
	if sec := d.Config.Security; sec != nil {
		cc.User = sec.User
		hc.CapAdd = sec.CapAdd
		hc.CapDrop = sec.CapDrop
		hc.GroupAdd = sec.GroupAdd
		hc.Tmpfs = sec.dockerTmpfs()
		hc.ReadonlyRootfs = sec.ReadOnlyRootfs
		hc.SecurityOpt = sec.dockerSecurityOpts()
		hc.Privileged = sec.Privileged
		hc.IpcMode = container.IpcMode(sec.IpcMode)
		hc.UsernsMode = container.UsernsMode(sec.UsernsMode)
		hc.Resources.PidsLimit = sec.dockerPidsLimit()
		hc.Resources.Ulimits = sec.dockerUlimits()

		if sec.Privileged {
			log.Printf("Warning: container %q runs privileged; most other security options are void\n", d.Config.Name)
		}
	}

	// requires either Image or Config.Image to be set, not both
	cco := client.ContainerCreateOptions{
		Config:     &cc,
		HostConfig: &hc,
		Name:       d.Config.Name,
	}

	resp, err := d.Client.ContainerCreate(ctx, cco)
	if err != nil {
		log.Printf("Error creating container using image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	// i had an issue with name reservation:
	// ContainerCreate already reserves the name in the daemon.
	// So if a later startup or anything else fails,
	// leaving the name reserved will break future startup attempts

	// if cleanup itself fails, we retain the ID
	// so the worker can attempt cleanup on the next pass
	cleanup := func(runErr error) DockerResult {
		_, removeErr := d.Client.ContainerRemove(ctx, resp.ID, client.ContainerRemoveOptions{
			RemoveVolumes: true,
			RemoveLinks:   false,
			Force:         true, // forsenW
		})
		if removeErr != nil {
			log.Printf("Error cleaning up container %s after startup failure: %v\n", resp.ID, removeErr)
			return DockerResult{
				ContainerID: resp.ID,
				Error:       fmt.Errorf("%w (also failed to clean up container: %v)", runErr, removeErr),
			}
		}
		return DockerResult{Error: runErr}
	}

	// As of now, ContainerStartResult contains no fields,
	// so I don't see any reason to store it
	_, err = d.Client.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return cleanup(err)
	}

	out, err := d.Client.ContainerLogs(
		ctx,
		resp.ID,
		client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return cleanup(err)
	}
	defer func() {
		err := out.Close()
		if err != nil {
			log.Printf("Error closing logs for container %s: %v\n", resp.ID, err)
		}
	}()

	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	if err != nil {
		log.Printf(
			"Error copying the stream to stdout&stderr for container %s: %v\n",
			resp.ID, err,
		)
		return cleanup(err)
	}

	return DockerResult{
		ContainerID: resp.ID,
		Action:      ActionStart,
		Result:      ResultSuccess,
	}
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)

	if id == "" {
		return DockerResult{Error: errors.New("cannot stop container: container ID is empty")}
	}

	ctx := context.Background()

	// As of now, ContainerStopResult contains no fields,
	// so I don't see any reason to store it
	_, stopErr := d.Client.ContainerStop(ctx, id, client.ContainerStopOptions{})
	if stopErr != nil {
		// a container that has already exited cannot always be stopped, but it can still be removed
		log.Printf("Error stopping container %s: %v\n", id, stopErr)
		if errdefs.IsNotFound(stopErr) {
			stopErr = nil
		}
	}

	var exitCode *int
	if resp, inspectErr := d.Client.ContainerInspect(ctx, id, client.ContainerInspectOptions{}); inspectErr == nil {
		if resp.Container.State != nil {
			code := resp.Container.State.ExitCode
			exitCode = &code
		}
	}

	// Same thing with ContainerStartResult and ContainerStopResult
	_, removeErr := d.Client.ContainerRemove(ctx, id, client.ContainerRemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         true,
	})
	if removeErr != nil {
		if errdefs.IsNotFound(removeErr) {
			return DockerResult{Action: ActionStop, Result: ResultSuccess, ExitCode: exitCode}
		}
		log.Printf("Error removing container %s: %v\n", id, removeErr)
		if stopErr != nil {
			return DockerResult{Error: fmt.Errorf("error stopping container: %w. error removing container: %v", stopErr, removeErr)}
		}
		return DockerResult{Error: removeErr}
	}

	return DockerResult{Action: ActionStop, Result: ResultSuccess, ExitCode: exitCode, Error: nil}
}
