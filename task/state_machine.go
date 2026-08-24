package task

import (
	"fmt"
	"slices"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

// implementing fmt.Stringer
func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Scheduled:
		return "Scheduled"
	case Running:
		return "Running"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Scheduled, Completed, Failed},

	// Completed to Scheduled: "always"/"unless-stopped" restart a cleanly exited task
	// Completed to Completed: a repeated stop stays idempotent
	Completed: {Scheduled, Completed},

	Failed: {Scheduled, Completed},
}

func ValidStateTransition(from, to State) bool {
	return slices.Contains(stateTransitionMap[from], to)
}
