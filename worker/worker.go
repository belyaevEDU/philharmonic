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

	"github.com/belyaevedu/philharmonic/queue"
	"github.com/belyaevedu/philharmonic/stats"
	"github.com/belyaevedu/philharmonic/store"
	"github.com/belyaevedu/philharmonic/task"
	"github.com/cakturk/go-netstat/netstat"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
)

const (
	DbFilemode = os.FileMode(0600)
)

var (
	DbFilename      = "%s.db" // %s is substituted with the worker's name
	DbBucketName    = "tasks"
	DbLogBucketName = "logs"

	LogCaptureMaxLines = 1000

	LoopInterval = 10 * time.Second
)

type Worker struct {
	Name  string
	Queue *queue.Queue[task.Task]
	Db    store.Store[task.Task]
	LogDb store.Store[task.TaskLogs]
	Stats *stats.Stats

	// stopTaskForForget runs the container stop when forgetting an active
	// task; overridden in tests. nil means StopTask
	stopTaskForForget func(t task.Task) task.DockerResult

	// owns the single bbolt file backing all three stores in bolt mode.
	// is nil for memory mode
	// the stores themselves are bucket views on it
	boltDb *store.SharedBolt

	dbMu    sync.RWMutex
	logMu   sync.Mutex
	statsMu sync.RWMutex

	// CPU usage is a rate, so it needs two samples of the cumulative /proc/stat counters
	prevCpuTotal uint64
	prevCpuIdle  uint64
	havePrevCpu  bool
}

func New(name, dbType string) (*Worker, error) {
	var db store.Store[task.Task]
	var logDb store.Store[task.TaskLogs]
	var boltDb *store.SharedBolt
	switch dbType {
	case store.MemoryType:
		db = store.NewInMemoryStore[task.Task]()
		logDb = store.NewInMemoryStore[task.TaskLogs]()
	case store.BoltType:
		sdb, dbErr := store.OpenSharedBolt(fmt.Sprintf(DbFilename, name), DbFilemode)
		if dbErr != nil {
			return nil, fmt.Errorf("opening worker db: %w", dbErr)
		}
		db, dbErr = store.Bucket[task.Task](sdb, DbBucketName)
		if dbErr != nil {
			return nil, errors.Join(fmt.Errorf("opening tasks bucket: %w", dbErr), sdb.Close())
		}
		logDb, dbErr = store.Bucket[task.TaskLogs](sdb, DbLogBucketName)
		if dbErr != nil {
			return nil, errors.Join(fmt.Errorf("opening logs bucket: %w", dbErr), sdb.Close())
		}
		boltDb = sdb
	default:
		return nil, fmt.Errorf("unknown db type %q", dbType)
	}

	return &Worker{
		Name:   name,
		Queue:  queue.New[task.Task](),
		Db:     db,
		LogDb:  logDb,
		boltDb: boltDb,
	}, nil
}

func (w *Worker) Close() error {
	w.dbMu.Lock()
	defer w.dbMu.Unlock()

	w.Db = nil
	w.LogDb = nil
	var err error
	if w.boltDb != nil {
		err = w.boltDb.Close()
		w.boltDb = nil
	}
	return err
}

func (w *Worker) listTasks() ([]*task.Task, error) {
	w.dbMu.RLock()
	defer w.dbMu.RUnlock()

	if w.Db == nil {
		return nil, errors.New("worker db is nil")
	}

	persisted, err := w.Db.List()
	if err != nil {
		return nil, err
	}

	tasks := make([]*task.Task, 0, len(persisted))
	for _, t := range persisted {
		if t == nil {
			continue
		}
		copy := *t
		tasks = append(tasks, &copy)
	}
	return tasks, nil
}

func (w *Worker) getTasks() []*task.Task {
	tasks, err := w.listTasks()
	if err != nil {
		log.Printf("Error listing worker tasks: %v\n", err)
		return []*task.Task{}
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

// resolves a UUID or name to a task via the shared task.ResolveRef
func (w *Worker) resolveTask(ref string) (t task.Task, found bool, ambiguous bool) {
	persisted := w.getTasks()
	tasks := make([]task.Task, 0, len(persisted))
	for _, p := range persisted {
		if p != nil {
			tasks = append(tasks, *p)
		}
	}
	return task.ResolveRef(tasks, ref)
}

func (w *Worker) captureLogs(t task.Task, exitCode *int, source string) {
	if t.ContainerID == "" {
		return
	}

	// don't rewrite an existing record: the first capture wins
	if _, exists := w.peekStoredLogs(t.ID); exists {
		return
	}

	config := task.NewConfig(&t)
	d, err := task.NewDocker(config)
	if err != nil {
		log.Printf("Error creating docker client to capture logs for task %s: %v\n", t.ID, err)
		return
	}

	logs, err := d.Logs(t.ContainerID, LogCaptureMaxLines)
	if err != nil {
		log.Printf("Error capturing logs for task %s: %v\n", t.ID, err)
		return
	}

	w.storeLogsIfAbsent(t.ID, logs, exitCode, source)
}

func (w *Worker) peekStoredLogs(id uuid.UUID) (task.TaskLogs, bool) {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.LogDb == nil {
		return task.TaskLogs{}, false
	}
	rec, err := w.LogDb.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error checking stored logs for task %s: %v\n", id, err)
		}
		return task.TaskLogs{}, false
	}
	return *rec, true
}

func (w *Worker) storeLogsIfAbsent(id uuid.UUID, logs []byte, exitCode *int, source string) {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.LogDb == nil {
		return
	}
	if existing, err := w.LogDb.Get(id); err == nil && existing != nil {
		return
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		log.Printf("Error checking stored logs for task %s: %v\n", id, err)
		return
	}
	rec := task.TaskLogs{
		TaskID:     id,
		ExitCode:   exitCode,
		Logs:       logs,
		CapturedAt: time.Now().UTC(),
		Source:     source,
	}
	if err := w.LogDb.Put(id, &rec); err != nil {
		log.Printf("Error storing captured logs for task %s: %v\n", id, err)
	}
}

func (w *Worker) getStoredLogs(id uuid.UUID) (task.TaskLogs, bool) {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.LogDb == nil {
		return task.TaskLogs{}, false
	}
	rec, err := w.LogDb.Get(id)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Error getting stored logs for task %s: %v\n", id, err)
		}
		return task.TaskLogs{}, false
	}
	return *rec, true
}

// called on restart so new container's lifecycle starts with a fresh capture
func (w *Worker) clearStoredLogs(id uuid.UUID) {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.LogDb == nil {
		return
	}
	if err := w.LogDb.Delete(id); err != nil {
		log.Printf("Error clearing stored logs for task %s: %v\n", id, err)
	}
}

type TaskLogsResult struct {
	Logs     []byte
	State    task.State
	ExitCode *int
	Live     bool // true = logs came from a live/exited container, false = from LogDb
}

func (w *Worker) GetTaskLogs(t task.Task, tail int) TaskLogsResult {
	if t.ContainerID != "" {
		config := task.NewConfig(&t)
		d, err := task.NewDocker(config)
		if err == nil {
			resp := d.Inspect(t.ContainerID)
			if resp.Error == nil && resp.Response != nil {
				logs, logErr := d.Logs(t.ContainerID, tail)
				if logErr == nil {
					var exitCode *int
					if resp.Response.State != nil {
						code := resp.Response.State.ExitCode
						exitCode = &code
					}
					return TaskLogsResult{Logs: logs, State: t.State, ExitCode: exitCode, Live: true}
				}
				// logs failed for a reason other than NotFound -> fall through
				// to stored logs rather than returning an error
			}
			// container gone -> fall through to stored logs
		}
	}

	if rec, ok := w.getStoredLogs(t.ID); ok {
		return TaskLogsResult{
			Logs:     tailLines(rec.Logs, tail),
			State:    t.State,
			ExitCode: rec.ExitCode,
			Live:     false,
		}
	}

	return TaskLogsResult{Logs: nil, State: t.State, ExitCode: nil, Live: false}
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

func (w *Worker) AddTask(t task.Task) error {
	if t.State != task.Completed {
		if err := task.ValidatePortMappings(t.Ports); err != nil {
			return err
		}
		if err := task.ValidateRestartPolicy(t.RestartPolicy); err != nil {
			return err
		}
		if t.Timeout < 0 {
			return fmt.Errorf("task timeout must not be negative, got %d", t.Timeout)
		}
		if t.MaxRestarts < 0 {
			return fmt.Errorf("task max_restarts must not be negative, got %d", t.MaxRestarts)
		}
	}

	if t.State == task.Scheduled {
		w.dbMu.Lock()
		if w.Db == nil {
			w.dbMu.Unlock()
			return errors.New("worker db is nil")
		}
		if persisted, err := w.Db.Get(t.ID); err == nil {
			if t.ContainerID == "" {
				t.ContainerID = persisted.ContainerID
			}
			if len(t.HostPorts) == 0 {
				t.HostPorts = persisted.HostPorts
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			w.dbMu.Unlock()
			return fmt.Errorf("getting worker task %s: %w", t.ID, err)
		}
		queued := t
		if err := w.Db.Put(queued.ID, &queued); err != nil {
			w.dbMu.Unlock()
			return fmt.Errorf("storing worker task %s: %w", queued.ID, err)
		}
		w.dbMu.Unlock()
	}

	w.Queue.Enqueue(t)
	return nil
}

func (w *Worker) queueLen() int {
	return w.Queue.Len()
}

// merges a queued event with the worker's persisted record for the task.
// the worker's container knowledge wins
func mergeQueuedWithPersisted(queued task.Task, persisted *task.Task) task.Task {
	if queued.ContainerID == "" {
		queued.ContainerID = persisted.ContainerID
	}
	// a stop must target the container this worker currently runs for the task
	if queued.State == task.Completed && persisted.ContainerID != "" {
		queued.ContainerID = persisted.ContainerID
	}
	return queued
}

// action is picked depending on the task's state
func (w *Worker) runTask() task.DockerResult {
	taskQueued, ok := w.Queue.Dequeue()
	if !ok {
		log.Println("No tasks in the queue")
		return task.DockerResult{Error: nil}
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
	taskQueued = mergeQueuedWithPersisted(taskQueued, taskPersisted)
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
	if t.RestartCount > 0 || t.ContainerID != "" {
		log.Printf("Restarting task %s (attempt %d)\n", t.ID, t.RestartCount)
		// a restart begins a new container lifecycle:
		// drop the previous lifecycle's captured logs
		// so they aren't served as the new one's
		w.clearStoredLogs(t.ID)
	} else {
		log.Printf("Starting task %s\n", t.ID)
	}

	if err := task.ValidatePortMappings(t.Ports); err != nil {
		t.State = task.Failed
		t.HostPorts = nil
		t.FailureReason = err.Error()
		w.setTask(t)
		return task.DockerResult{Error: err}
	}

	if err := task.ValidateRestartPolicy(t.RestartPolicy); err != nil {
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

	// grab the container's output while it still exists
	logs, _ := d.Logs(t.ContainerID, LogCaptureMaxLines)

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

	if logs != nil {
		w.storeLogsIfAbsent(t.ID, logs, result.ExitCode, "stop")
	}

	if result.Error == nil {
		log.Printf("Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)
	} else {
		log.Printf("Could not stop and remove container %v for task %v\n", t.ContainerID, t.ID)
	}

	return result
}

func (w *Worker) ForgetTask(id uuid.UUID) error {
	w.dbMu.RLock()
	if w.Db == nil {
		w.dbMu.RUnlock()
		return errors.New("worker db is nil")
	}
	persisted, err := w.Db.Get(id)
	if err != nil {
		w.dbMu.RUnlock()
		return err // ErrNotFound usually
	}
	persistedTask := *persisted
	active := persistedTask.State == task.Scheduled || persistedTask.State == task.Running
	w.dbMu.RUnlock()

	// drop queued events for every lifecycle state
	w.Queue.RemoveAllFunc(func(t task.Task) bool {
		return t.ID == id
	})

	if active {
		if persistedTask.ContainerID != "" {
			// stop takes dbmu itself
			stop := w.stopTaskForForget
			if stop == nil {
				stop = w.StopTask
			}
			if result := stop(persistedTask); result.Error != nil {
				return fmt.Errorf("stopping active task %s before forget: %w", id, result.Error)
			}
		}
	}

	w.dbMu.Lock()
	defer w.dbMu.Unlock()

	if w.Db == nil {
		return errors.New("worker db is nil")
	}

	// re-read: the record may have changed (or vanished) while the stop ran
	current, err := w.Db.Get(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // already gone
		}
		return err
	}
	if current.ContainerID != "" && (current.State == task.Scheduled || current.State == task.Running) {
		// something reactivated the task while the stop ran.
		// deleting now would orphan a live container.
		// the manager's next reconciliation retries the forget against the reactivated record
		return fmt.Errorf("task %s became active again while being forgotten", id)
	}
	if err := w.Db.Delete(id); err != nil {
		return err
	}

	// lock ordering: dbMu -> logMu. nothing takes them the other way around
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.LogDb != nil {
		if err := w.LogDb.Delete(id); err != nil {
			log.Printf("Error deleting stored logs for forgotten task %s: %v\n", id, err)
		}
	}
	return nil
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

func (w *Worker) PullImage(image string) (bool, error) {
	// only the Image field is relevant for a pull
	d, err := task.NewDocker(&task.Config{Image: image})
	if err != nil {
		return false, fmt.Errorf("creating docker client: %w", err)
	}
	return d.Pull(image)
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
		if taskTimedOut(runningTask) {
			w.timeoutTask(runningTask)
			continue
		}

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
		var (
			captureTask   task.Task
			captureExit   *int
			captureSource string
			shouldCapture bool
		)
		if resp.Response.State.Status == container.StateExited {
			code := resp.Response.State.ExitCode
			captureExit = &code
			captureSource = "exit"
			shouldCapture = true
			if code == 0 {
				log.Printf("Container for task %s exited successfully (code 0)", runningTask.ID)
				updated.State = task.Completed
				updated.FinishTime = time.Now().UTC()
				updated.FailureReason = ""
			} else {
				log.Printf(
					"Container for task %s in non-running state %s",
					runningTask.ID, resp.Response.State.Status,
				)
				updated.State = task.Failed
				updated.FailureReason = fmt.Sprintf("container exited with code %d", code)
			}
		} else if resp.Response.State.Health != nil && resp.Response.State.Health.Status == container.Unhealthy {
			log.Printf("Container for task %s is unhealthy", runningTask.ID)
			updated.State = task.Failed
			updated.FailureReason = healthCheckFailureReason(resp.Response.State.Health)
			captureSource = "unhealthy"
			shouldCapture = true
		}

		if resp.Response.NetworkSettings != nil {
			updated.HostPorts = task.PortMappingsFromPortMap(resp.Response.NetworkSettings.Ports)
		}
		if err := w.Db.Put(runningTask.ID, &updated); err != nil {
			log.Printf("error storing task %s: %v\n", runningTask.ID, err)
		}
		if shouldCapture {
			captureTask = updated
		}
		w.dbMu.Unlock()

		if shouldCapture {
			w.captureLogs(captureTask, captureExit, captureSource)
		}
	}
}

func taskTimedOut(t task.Task) bool {
	if t.Timeout <= 0 || t.StartTime.IsZero() {
		return false
	}
	return time.Since(t.StartTime) > time.Duration(t.Timeout)*time.Second
}

func (w *Worker) timeoutTask(t task.Task) {
	log.Printf("Task %s exceeded its timeout of %ds; stopping container %s\n", t.ID, t.Timeout, t.ContainerID)

	if t.ContainerID != "" {
		config := task.NewConfig(&t)
		if d, err := task.NewDocker(config); err == nil {
			logs, _ := d.Logs(t.ContainerID, LogCaptureMaxLines)
			if result := d.Stop(t.ContainerID); result.Error != nil {
				log.Printf("Error stopping timed out container %s for task %s: %v\n", t.ContainerID, t.ID, result.Error)
			}
			if logs != nil {
				w.storeLogsIfAbsent(t.ID, logs, nil, "timeout")
			}
		} else {
			log.Printf("Error creating docker client to stop timed out task %s: %v\n", t.ID, err)
		}
	}

	w.dbMu.Lock()
	defer w.dbMu.Unlock()

	if w.Db == nil {
		return
	}
	persisted, err := w.Db.Get(t.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("error getting timed out task %s: %v\n", t.ID, err)
		}
		return
	}
	if persisted.State != task.Running {
		// a stop or restart raced the timeout, its outcome wins
		return
	}
	updated := *persisted
	updated.State = task.Failed
	updated.FailureReason = fmt.Sprintf("task timed out after %d seconds", t.Timeout)
	updated.HostPorts = nil
	// no FinishTime: a timeout is a retryable failure, and Failed +
	// FinishTime is the manager's terminal stamp; a timed-out task must
	// stay restartable until it reaches its restart cap
	if err := w.Db.Put(updated.ID, &updated); err != nil {
		log.Printf("error storing timed out task %s: %v\n", updated.ID, err)
	}
}

func (w *Worker) UpdateTasks(ctx context.Context) {
	ticker := time.NewTicker(LoopInterval)
	defer ticker.Stop()
	for {
		w.updateTasks()
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

// request/response bodies for POST /images (pull an image on this worker)
type PullImageRequest struct {
	Image string `json:"image"`
}

type PullImageResponse struct {
	Image  string `json:"image"`
	Pulled bool   `json:"pulled"` // false = the image was already present locally
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

// used to honor the ?tail= query param on stored logs
func tailLines(blob []byte, n int) []byte {
	if n <= 0 || len(blob) == 0 {
		return blob
	}

	start := 0
	lines := 0
	for i := len(blob) - 1; i >= 0; i-- {
		if blob[i] == '\n' && i+1 < len(blob) {
			lines++
			if lines >= n {
				start = i + 1
				break
			}
		}
	}
	if lines < n {
		return blob
	}
	return blob[start:]
}
