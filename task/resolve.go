package task

import "github.com/google/uuid"

// resolves a task reference (UUID or name) against a snapshot of tasks
func ResolveRef(tasks []Task, ref string) (match Task, found bool, ambiguous bool) {
	if id, err := uuid.Parse(ref); err == nil {
		for _, t := range tasks {
			if t.ID == id {
				return t, true, false
			}
		}
		return Task{}, false, false
	}

	count := 0
	for _, t := range tasks {
		if t.Name == ref {
			match = t
			count++
		}
	}
	return match, count == 1, count > 1
}
