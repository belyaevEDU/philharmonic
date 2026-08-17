package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/belyaevedu/philharmonic/stats"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/cakturk/go-netstat/netstat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
)

const (
	DbFilename   = "%s_tasks.db"
	DbFilemode   = os.FileMode(0600)
	DbBucketName = "tasks"

	LoopInterval = 10 * time.Second
)

type Worker struct {
	Name  string
	Queue queue.Queue
	Db    store.Store[task.Task]
	Stats *stats.Stats

	dbMu    sync.RWMutex
	queueMu sync.Mutex
	statsMu sync.RWMutex

	// CPU usage is a rate, so it needs two samples of the cumulative /proc/stat counters
	prevCpuTotal uint64
	prevCpuIdle  uint64
	havePrevCpu  bool
}

func New(name, dbType string) (*Worker, error) {
	var db store.Store[task.Task]
	var err error
	switch dbType {
	case store.MemoryType:
		db = store.NewInMemoryStore[task.Task]()
	case store.BoltType:
		filename := fmt.Sprintf(DbFilename, name)
		db, err = store.NewBoltStore[task.Task](filename, DbFilemode, DbBucketName)
	default:
		return nil, fmt.Errorf("unknown db type %q", dbType)
	}

	if err != nil {
		return nil, err
	}

	return &Worker{
		Name:  name,
		Queue: *queue.New(),
		Db:    db,
	}, nil
}

func (w *Worker) Close() error {
	w.dbMu.Lock()
	defer w.dbMu.Unlock()
	if w.Db == nil {
		return nil
	}
	err := w.Db.Close()
	w.Db = nil
	return err
}

func (w *Worker) getTasks() []*task.Task {
	w.dbMu.RLock()
	defer w.dbMu.RUnlock()

	if w.Db == nil {
		return []*task.Task{}
	}

	persisted, err := w.Db.List()
	if err != nil {
		log.Printf("Error listing worker tasks: %v\n", err)
		return []*task.Task{}
	}

	tasks := make([]*task.Task, 0, len(persisted))
	for _, t := range persisted {
		if t == nil {
			continue
		}
		copy := *t
		tasks = append(tasks, &copy)
	}
	return tasks
}

func (w *Worker) getTask(id uuid.UUID) (task.Task, bool) {
	w.dbMu.RLock()
	defer w.dbMu.RUnlock()

	if w.Db == nil {
		return task.Task{}, false
	}

	persisted, err := w.Db.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error getting worker task %s: %v\n", id, err)
		}
		return task.Task{}, false
	}
	return *persisted, true
}

func (w *Worker) setTask(t task.Task) {
	w.dbMu.Lock()
	defer w.dbMu.Unlock()

	if w.Db == nil {
		log.Printf("Cannot store worker task %s: db is nil\n", t.ID)
		return
	}
	if err := w.Db.Put(t.ID, &t); err != nil {
		log.Printf("Error storing worker task %s: %v\n", t.ID, err)
	}
}

func (w *Worker) AddTask(t task.Task) {
	// now publishing a scheduled start before putting the task in queue
	// so that the manager can't get a Failed state from any prev. attempt

	// also saving the last known container ID
	// so StartTask can then remove that container before creating the replacement

	if t.State == task.Scheduled {
		w.dbMu.Lock()
		if w.Db == nil {
			log.Printf("Cannot store scheduled worker task %s: db is nil\n", t.ID)
		} else {
			if persisted, err := w.Db.Get(t.ID); err == nil {
				if t.ContainerID == "" {
					t.ContainerID = persisted.ContainerID
				}
				if len(t.HostPorts) == 0 {
					t.HostPorts = persisted.HostPorts
				}
			} else if !errors.Is(err, store.ErrNotFound) {
				log.Printf("Error getting worker task %s: %v\n", t.ID, err)
			}
			queued := t
			if err := w.Db.Put(queued.ID, &queued); err != nil {
				log.Printf("Error storing worker task %s: %v\n", queued.ID, err)
			}
		}
		w.dbMu.Unlock()
	}

	w.queueMu.Lock()
	w.Queue.Enqueue(t)
	w.queueMu.Unlock()
}

func (w *Worker) queueLen() int {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	return w.Queue.Len()
}

func (w *Worker) dequeueTask() any {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	return w.Queue.Dequeue()
}

// action is picked depending on the task's state
func (w *Worker) runTask() task.DockerResult {
	t := w.dequeueTask()
	if t == nil {
		log.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued, ok := t.(task.Task)
	if !ok {
		return task.DockerResult{
			Error: errors.New(
				"error pulling a task off the queue: somehow there's a non-task.Task element",
			),
		}
	}

	w.dbMu.Lock()
	if w.Db == nil {
		w.dbMu.Unlock()
		return task.DockerResult{Error: errors.New("worker db is nil")}
	}

	taskPersisted, err := w.Db.Get(taskQueued.ID)
	if errors.Is(err, store.ErrNotFound) {
		// w.Db has to store the CURRENT state of the container
		// so we update it if its new and scheduled
		// all other changes will be stored by startTask and stopTask down the line

		if taskQueued.State != task.Scheduled {
			w.dbMu.Unlock()
			return task.DockerResult{
				Error: fmt.Errorf(
					"task %s is not known to this worker (state %v); refusing to process",
					taskQueued.ID.String(), taskQueued.State,
				),
			}
		}

		persisted := taskQueued
		taskPersisted = &persisted
		if err := w.Db.Put(taskQueued.ID, taskPersisted); err != nil {
			w.dbMu.Unlock()
			return task.DockerResult{Error: fmt.Errorf("storing worker task %s: %w", taskQueued.ID, err)}
		}
	} else if err != nil {
		w.dbMu.Unlock()
		return task.DockerResult{Error: fmt.Errorf("getting worker task %s: %w", taskQueued.ID, err)}
	}

	// doubling prevention.
	// in these stages, considering the fact that manager updates task via 10 second period,
	// we prioritize the info stored on the worker
	if taskQueued.ContainerID == "" {
		taskQueued.ContainerID = taskPersisted.ContainerID
	}
	persistedState := taskPersisted.State
	w.dbMu.Unlock()

	var result task.DockerResult
	if task.ValidStateTransition(persistedState, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = fmt.Errorf(
				"error when trying to run task %s: state machine failed on state %v",
				taskQueued.ID.String(), taskQueued.State,
			)
		}
	} else {
		result.Error = fmt.Errorf(
			"invalid transition from %v to %v for task %s",
			persistedState, taskQueued.State, taskQueued.ID.String(),
		)
	}
	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	if err := task.ValidatePortMappings(t.Ports); err != nil {
		t.State = task.Failed
		t.HostPorts = nil
		t.FailureReason = err.Error()
		w.setTask(t)
		return task.DockerResult{Error: err}
	}

	t.StartTime = time.Now().UTC()
	t.HostPorts = nil

	config := task.NewConfig(&t)

	d, err := task.NewDocker(config)
	if err != nil {
		t.State = task.Failed
		t.FailureReason = fmt.Sprintf("could not create docker client: %v", err)
		w.setTask(t)
		return task.DockerResult{Error: err}
	}

	if t.ContainerID != "" {
		log.Printf("Removing container %s of task %s before (re)starting it\n", t.ContainerID, t.ID)
		stopResult := d.Stop(t.ContainerID)
		if stopResult.Error != nil {
			log.Printf("Error removing previous container %s for task %s: %v\n", t.ContainerID, t.ID, stopResult.Error)
			t.State = task.Failed
			t.FailureReason = fmt.Sprintf("could not remove previous container: %v", stopResult.Error)
			w.setTask(t)
			return stopResult
		}
		t.ContainerID = ""
	}

	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		if result.ContainerID != "" {
			t.ContainerID = result.ContainerID
		}
		t.State = task.Failed
		t.FailureReason = fmt.Sprintf("container failed to start: %v", result.Error)
		w.setTask(t)
		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.Running

	inspect := d.Inspect(result.ContainerID)
	if inspect.Error != nil {
		log.Printf("Error inspecting container %s after start: %v\n", result.ContainerID, inspect.Error)
	} else if inspect.Response == nil || inspect.Response.NetworkSettings == nil {
		log.Printf("Container %s returned no network settings after start\n", result.ContainerID)
	} else {
		t.HostPorts = task.PortMappingsFromPortMap(inspect.Response.NetworkSettings.Ports)
	}

	w.setTask(t)

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)

	d, err := task.NewDocker(config)
	if err != nil {
		return task.DockerResult{Error: err}
	}

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}

	t.FinishTime = time.Now().UTC()

	persisted, exists := w.getTask(t.ID)
	if !exists {
		msg := fmt.Sprintf("error: task with ID %s isn't in the worker db", t.ID.String())
		fmt.Println(msg)
		return task.DockerResult{Error: errors.New(msg)}
	}

	switch {
	case t.FailureReason != "":
		t.State = task.Failed
	case persisted.State == task.Failed: // worker ahead of mngr fallback
		t.State = task.Failed
		t.FailureReason = persisted.FailureReason
	default:
		t.State = task.Completed
	}

	w.setTask(t)

	if result.Error == nil {
		log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	} else {
		log.Printf("Could not stop and remove container %v for task %v\n", t.ContainerID, t.ID)
	}

	return result
}

func (w *Worker) RunTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		if w.queueLen() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Println("No tasks to process currently.")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) CollectStats(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		log.Println("Collecting stats...")
		fresh := stats.GetStats()

		// CPU usage is measured as the work done between this sample and the
		// previous one. /proc/stat counters are cumulative since boot, so the
		// delta over the ~10s collection interval is the actual util
		idle, nonIdle := fresh.CpuUsageSplit()
		total := idle + nonIdle
		var cpuUsage float64
		if w.havePrevCpu && total >= w.prevCpuTotal && idle >= w.prevCpuIdle {
			dTotal := total - w.prevCpuTotal
			if dTotal > 0 {
				dIdle := idle - w.prevCpuIdle
				if dIdle > dTotal {
					dIdle = dTotal // paranoia: keep the fraction in [0, 1] :/
				}
				cpuUsage = float64(dTotal-dIdle) / float64(dTotal)
			}
		}
		w.prevCpuTotal = total
		w.prevCpuIdle = idle
		w.havePrevCpu = true
		fresh.CpuUsage = cpuUsage
		fresh.Cores = runtime.NumCPU()

		persistedTasks := w.getTasks()
		var (
			taskCount       int
			memoryAllocated int64
		)
		for _, persisted := range persistedTasks {
			if persisted.State != task.Running {
				continue
			}
			taskCount++
			memoryAllocated += persisted.Memory
		}
		fresh.TaskCount = taskCount
		fresh.MemoryAllocated = memoryAllocated

		w.statsMu.Lock()
		w.Stats = fresh
		w.statsMu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d, err := task.NewDocker(config)
	if err != nil {
		return task.DockerInspectResponse{Error: err}
	}
	return d.Inspect(t.ContainerID)
}

func healthCheckFailureReason(health *container.Health) string {
	if health != nil && len(health.Log) > 0 {
		last := health.Log[len(health.Log)-1]
		if last != nil {
			output := strings.TrimSpace(last.Output)
			if output != "" {
				return fmt.Sprintf("docker healthcheck failed with exit code %d: %s", last.ExitCode, output)
			}

			return fmt.Sprintf("docker healthcheck failed with exit code %d", last.ExitCode)
		}
	}

	if health != nil {
		return fmt.Sprintf(
			"container reported unhealthy by the docker health check (failing streak: %d)",
			health.FailingStreak,
		)
	}

	return "container reported unhealthy by the docker health check"
}

func (w *Worker) updateTasks() {
	persistedTasks := w.getTasks()
	runningTasks := make([]task.Task, 0, len(persistedTasks))
	for _, persisted := range persistedTasks {
		if persisted.State == task.Running {
			runningTasks = append(runningTasks, *persisted)
		}
	}

	for _, runningTask := range runningTasks {
		resp := w.InspectTask(runningTask)
		if resp.Error != nil {
			log.Printf("error updating task %s: %v\n", runningTask.ID, resp.Error)
		}

		w.dbMu.Lock()
		if w.Db == nil {
			w.dbMu.Unlock()
			continue
		}
		persisted, err := w.Db.Get(runningTask.ID)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				log.Printf("error getting worker task %s: %v\n", runningTask.ID, err)
			}
			w.dbMu.Unlock()
			continue
		}
		if persisted.State != task.Running {
			w.dbMu.Unlock()
			continue
		}

		updated := *persisted
		if resp.Response == nil {
			updated.State = task.Failed
			updated.HostPorts = nil
			if resp.Error != nil {
				updated.FailureReason = fmt.Sprintf("container inspection failed: %v", resp.Error)
			} else {
				updated.FailureReason = "no container found for running task"
			}
			if err := w.Db.Put(runningTask.ID, &updated); err != nil {
				log.Printf("error storing failed task %s: %v\n", runningTask.ID, err)
			}
			w.dbMu.Unlock()
			continue
		}

		if resp.Response.State == nil {
			updated.State = task.Failed
			updated.HostPorts = nil
			updated.FailureReason = "container inspection returned no state"
			if err := w.Db.Put(runningTask.ID, &updated); err != nil {
				log.Printf("error storing failed task %s: %v\n", runningTask.ID, err)
			}
			w.dbMu.Unlock()
			continue
		}

		updated.HostPorts = nil
		if resp.Response.State.Status == container.StateExited {
			log.Printf(
				"Container for task %s in non-running state %s",
				runningTask.ID, resp.Response.State.Status,
			)
			updated.State = task.Failed
			updated.FailureReason = fmt.Sprintf("container exited with code %d", resp.Response.State.ExitCode)
		} else if resp.Response.State.Health != nil && resp.Response.State.Health.Status == container.Unhealthy {
			log.Printf("Container for task %s is unhealthy", runningTask.ID)
			updated.State = task.Failed
			updated.FailureReason = healthCheckFailureReason(resp.Response.State.Health)
		}

		if resp.Response.NetworkSettings != nil {
			updated.HostPorts = task.PortMappingsFromPortMap(resp.Response.NetworkSettings.Ports)
		}
		if err := w.Db.Put(runningTask.ID, &updated); err != nil {
			log.Printf("error storing task %s: %v\n", runningTask.ID, err)
		}
		w.dbMu.Unlock()
	}
}

func (w *Worker) UpdateTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("Sleeping for 10 seconds...")
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type OccupiedPorts struct {
	TCP []int `json:"tcp,omitempty"`
	UDP []int `json:"udp,omitempty"`
}

func (w *Worker) HostPortsWithError() (OccupiedPorts, error) {
	return w.hostPorts()
}

func (w *Worker) hostPorts() (OccupiedPorts, error) {
	var occ OccupiedPorts
	var errs []error
	tcpSeen := make(map[int]struct{})
	udpSeen := make(map[int]struct{})

	addTCP := func(port int) {
		if port != 0 {
			if _, exists := tcpSeen[port]; !exists {
				tcpSeen[port] = struct{}{}
				occ.TCP = append(occ.TCP, port)
			}
		}
	}
	addUDP := func(port int) {
		if port != 0 {
			if _, exists := udpSeen[port]; !exists {
				udpSeen[port] = struct{}{}
				occ.UDP = append(occ.UDP, port)
			}
		}
	}

	tcpListen := func(s *netstat.SockTabEntry) bool {
		return s.State == netstat.Listen && s.LocalAddr != nil
	}
	for _, fn := range []func(netstat.AcceptFn) ([]netstat.SockTabEntry, error){
		netstat.TCPSocks, netstat.TCP6Socks,
	} {
		entries, err := fn(tcpListen)
		if err != nil {
			errs = append(errs, fmt.Errorf("reading TCP listening sockets: %w", err))
			continue
		}
		for _, e := range entries {
			if e.LocalAddr != nil {
				addTCP(int(e.LocalAddr.Port))
			}
		}
	}

	for _, fn := range []func(netstat.AcceptFn) ([]netstat.SockTabEntry, error){
		netstat.UDPSocks, netstat.UDP6Socks,
	} {
		entries, err := fn(netstat.NoopFilter)
		if err != nil {
			errs = append(errs, fmt.Errorf("reading UDP sockets: %w", err))
			continue
		}
		for _, e := range entries {
			if e.LocalAddr != nil {
				addUDP(int(e.LocalAddr.Port))
			}
		}
	}

	// a scheduled task is a reservation even before docker creates its container
	persistedTasks, err := w.listTasks()
	if err != nil {
		errs = append(errs, fmt.Errorf("listing worker tasks: %w", err))
	}
	for _, persisted := range persistedTasks {
		if persisted.State != task.Scheduled && persisted.State != task.Running {
			continue
		}

		var mappings []task.PortMapping
		if persisted.State == task.Scheduled {
			mappings = persisted.Ports
		} else {
			pinned := false
			for _, pm := range persisted.Ports {
				if pm.HostPort != 0 {
					pinned = true
					break
				}
			}
			if !pinned {
				continue
			}

			// do not trust the last persisted HostPorts for a running task.
			// an exited container may leave that field behind and cause the manager
			// to self-exclude a port now owned by some other process.
			// inspecting also catches Docker NAT bindings that have no host LISTEN socket
			inspect := w.InspectTask(*persisted)
			if inspect.Error != nil || inspect.Response == nil || inspect.Response.State == nil {
				if inspect.Error != nil {
					errs = append(errs, fmt.Errorf("inspecting task %s: %w", persisted.ID, inspect.Error))
				} else {
					errs = append(errs, fmt.Errorf("inspecting task %s returned no container state", persisted.ID))
				}
				continue
			}
			if inspect.Response.State.Status != container.StateRunning &&
				inspect.Response.State.Status != container.StateRestarting {
				// updateTasks will reconcile this stale Running record
				continue
			}
			if inspect.Response.NetworkSettings == nil {
				errs = append(errs, fmt.Errorf("inspecting task %s returned no network settings", persisted.ID))
				continue
			}
			mappings = task.PortMappingsFromPortMap(inspect.Response.NetworkSettings.Ports)
		}

		for _, pm := range mappings {
			if pm.HostPort == 0 {
				continue
			}
			switch pm.Protocol {
			case "", "tcp":
				addTCP(pm.HostPort)
			case "udp":
				addUDP(pm.HostPort)
			default:
				return OccupiedPorts{}, errors.New("non ''/'tcp'/'udp' mapping entry ended up in the portmappings slice")
			}
		}
	}

	sort.Ints(occ.TCP)
	sort.Ints(occ.UDP)
	return occ, errors.Join(errs...)
}
