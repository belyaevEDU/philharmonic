package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

type Action string
type Result string

const (
	ActionStart   Action = "start"
	ActionStop    Action = "stop"
	ResultSuccess Result = "success"
)

type Task struct {
	ID            uuid.UUID
	Name          string
	State         State
	Image         string
	Memory        int
	Disk          int
	ExposedPorts  network.PortSet
	PortBindings  map[string]string
	RestartPolicy string
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
	ExposedPorts  network.PortSet
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

type Docker struct {
	Client *client.Client
	Config Config
}

type DockerResult struct {
	ContainerId string
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
		log.Printf("Error copying the reader for ImagePull to stdout for image %s: %v\n", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	rp := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
	}

	r := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	cc := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true, // might want to redo that later
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
		log.Printf("Error copying the stream to stdout&stderr for container %s: %v\n", resp.ID, err)
		return DockerResult{Error: err}
	}

	return DockerResult{
		ContainerId: resp.ID,
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
