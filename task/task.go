package task

import (
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
	"time"

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

type HealthCheckType string

const (
	HealthCheckHTTP HealthCheckType = "http"
	HealthCheckTCP  HealthCheckType = "tcp"
	HealthCheckExec HealthCheckType = "exec"

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
	RestartPolicy string
	HostPorts     []PortMapping // resolved bindings reported by the daemon
	HealthCheck   *HealthCheck
	RestartCount  int
	StartTime     time.Time
	FinishTime    time.Time
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
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
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		Ports:         t.Ports,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
		HealthCheck:   t.HealthCheck,
	}
}

func (c *Config) dockerPorts() (network.PortSet, network.PortMap, error) {
	exposed := network.PortSet{}
	bindings := network.PortMap{}

	for _, pm := range c.Ports {
		if pm.ContainerPort < 1 || pm.ContainerPort > 65535 {
			return nil, nil, fmt.Errorf("invalid container port %d in port mapping", pm.ContainerPort)
		}
		if pm.HostPort < 0 || pm.HostPort > 65535 {
			return nil, nil, fmt.Errorf("invalid host port %d in port mapping", pm.HostPort)
		}

		proto := network.TCP
		if pm.Protocol != "" {
			proto = pm.Protocol
		}

		if proto != network.TCP && proto != network.UDP && proto != network.SCTP {
			return nil, nil, fmt.Errorf(
				"invalid protocol %s in port mapping (want tcp, udp or sctp)", pm.Protocol,
			)
		}

		port, ok := network.PortFrom(uint16(pm.ContainerPort), proto)
		if !ok {
			return nil, nil, fmt.Errorf("invalid port mapping %d/%s", pm.ContainerPort, proto)
		}

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
	Error       error
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, client.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		log.Printf(
			"Error copying the reader for ImagePull to stdout for image %s: %v\n",
			d.Config.Image, err,
		)
		return DockerResult{Error: err}
	}

	rp := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
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
		Env:          d.Config.Env,
		ExposedPorts: exposed,
		Healthcheck:  d.Config.dockerHealthcheck(),
	}

	hc := container.HostConfig{
		RestartPolicy: rp,
		Resources:     r,
		PortBindings:  bindings,
	}

	// requires either Image or Config.Image to be set, not both
	cco := client.ContainerCreateOptions{
		Config:     &cc,
		HostConfig: &hc,
		Name:       d.Config.Name,
	}

	resp, err := d.Client.ContainerCreate(ctx, cco)
	if err != nil {
		log.Printf("Error creating container using image %s, %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	// As of now, ContainerStartResult contains no fields,
	// so I don't see any reason to store it
	_, err = d.Client.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	out, err := d.Client.ContainerLogs(
		ctx,
		resp.ID,
		client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	if err != nil {
		log.Printf("Error getting logs for container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	if err != nil {
		log.Printf(
			"Error copying the stream to stdout&stderr for container %s: %v\n",
			resp.ID, err,
		)
		return DockerResult{Error: err}
	}

	return DockerResult{
		ContainerID: resp.ID,
		Action:      ActionStart,
		Result:      ResultSuccess,
	}
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)

	ctx := context.Background()

	// As of now, ContainerStopResult contains no fields,
	// so I don't see any reason to store it
	_, err := d.Client.ContainerStop(ctx, id, client.ContainerStopOptions{})
	if err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)
		return DockerResult{Error: err}
	}

	// Same thing with ContainerStartResult and ContainerStopResult
	_, err = d.Client.ContainerRemove(ctx, id, client.ContainerRemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         false,
	})
	if err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)
	}

	return DockerResult{Action: ActionStop, Result: ResultSuccess, Error: nil}
}
