package daemon

import (
	"context"
	"time"

	"github.com/goinsane/xcontext"
)

type Job interface {
	Id() int
	Init(dmn *Daemon, sendEvent SendEventFunc) error
	Loop(ctx context.Context, tm time.Time) (ok bool, err error)
}

type JobData struct {
	Job     Job
	InLoop  bool
	Stopped bool
	Err     error

	*xcontext.Locker

	notificationEvents map[string][]chan<- Event
}

func NewJobData(job Job) *JobData {
	return &JobData{
		Job:                job,
		Locker:             &xcontext.Locker{},
		notificationEvents: make(map[string][]chan<- Event, 16),
	}
}

func (d *JobData) Duplicate() *JobData {
	r := *d
	r.Locker = &xcontext.Locker{}
	r.notificationEvents = make(map[string][]chan<- Event, len(d.notificationEvents))
	for key, val := range d.notificationEvents {
		r.notificationEvents[key] = make([]chan<- Event, len(val))
		copy(r.notificationEvents[key], val)
	}
	return &r
}

func (d *JobData) DuplicateWithLock(ctx context.Context) (*JobData, error) {
	err := d.LockContext(ctx)
	if err != nil {
		return nil, err
	}
	defer d.Unlock()
	return d.Duplicate(), nil
}

type SendEventFunc func(eventName string, eventData interface{}) error
