package main

import (
	"fmt"
	"os"
	"time"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/moby/moby/client"
)

func main() {
	dockerTask, createResult := createContainer()
	if createResult != nil && createResult.Error != nil {
		fmt.Printf("%v", createResult.Error)
		os.Exit(1)
	}

	fmt.Println("stopping")

	time.Sleep(time.Second * 5)
	fmt.Printf("stopping cntnr %s\n", createResult.ContainerID)
	if dockerTask == nil {
		fmt.Println("dockertask is nil")
	}
	_ = stopContainer(dockerTask, createResult.ContainerID)
}

func createContainer() (*task.Docker, *task.DockerResult) {
	c := task.Config{
		Name:  "test-container-1",
		Image: "postgres:latest",
		Env: []string{
			"POSTGRES_USER=philharmonic_cube",
			"POSTGRES_PASSWORD=secret",
		},
	}

	dc, _ := client.New(client.FromEnv)
	d := task.Docker{
		Client: dc,
		Config: c,
	}

	result := d.Run()
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil, nil
	}

	fmt.Printf("Container %s is running with config %v\n", result.ContainerID, c)
	return &d, &result
}

func stopContainer(d *task.Docker, id string) *task.DockerResult {
	result := d.Stop(id)
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil
	}

	fmt.Printf("Container %s has been stopped and removed\n", result.ContainerID)
	return &result
}
