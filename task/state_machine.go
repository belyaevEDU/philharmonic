package task

import "slices"

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Scheduled, Completed, Failed},
	Completed: {},
	Failed:    {Scheduled},
}

func ValidStateTransition(from, to State) bool {
	return slices.Contains(stateTransitionMap[from], to)
}
