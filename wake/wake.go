package wake

type Waker struct {
	C chan struct{}
}

func NewWaker() Waker {
	return Waker{C: make(chan struct{}, 1)}
}

// Wake triggers the waiting loop.
func (w Waker) Wake() {
	if w.C == nil {
		return
	}
	select {
	case w.C <- struct{}{}:
	default: // a signal is already pending
	}
}
