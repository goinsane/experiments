package daemon

import (
	"context"
)

func FunctionNotifier(ctx context.Context, bufSize int, f func(Event)) chan<- Event {
	if bufSize < 0 {
		bufSize = 0
	}
	result := make(chan Event, bufSize)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-result:
				f(event)
			}
		}
	}()
	return result
}
