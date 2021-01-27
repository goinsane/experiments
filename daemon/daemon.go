package daemon

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goinsane/application"
	"github.com/goinsane/xlog"
)

type Daemon struct {
	ErrorLogger     *xlog.Logger
	InfoLogger      *xlog.Logger
	LoopPeriod      time.Duration
	LoopPeriodShift time.Duration

	running  int32
	inLoop   int32
	mu       sync.RWMutex
	eventsCh chan Event
	jobDatas map[int]*JobData
}

func (d *Daemon) Start(ctx application.Context) {
	_ = ctx
}

func (d *Daemon) Run(ctx application.Context) {
	if !atomic.CompareAndSwapInt32(&d.running, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&d.running, 0)
	defer ctx.Terminate()
	d.Loop(ctx)
}

func (d *Daemon) Terminate(ctx context.Context) {
	_ = ctx
}

func (d *Daemon) Stop() {
}

func (d *Daemon) Loop(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&d.inLoop, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&d.inLoop, 0)

	d.mu.Lock()
	d.initialize()
	eventsCh := d.eventsCh
	d.mu.Unlock()

	eventLoopWg := new(sync.WaitGroup)
	eventLoopWg.Add(1)
	go d.eventLoop(eventLoopWg, eventsCh)

	periodSec := int64(d.LoopPeriod.Round(time.Second) / time.Second)
	if periodSec <= 0 {
		periodSec = 1
	}
	periodShiftSec := int64(d.LoopPeriodShift.Round(time.Second) / time.Second)

	tckr := time.NewTicker(time.Second)
	defer tckr.Stop()
	wg := new(sync.WaitGroup)
	for ctx.Err() == nil {
		var done bool
		var tm time.Time
		select {
		case <-ctx.Done():
			done = true
		case tm = <-tckr.C:
		}
		if done {
			break
		}

		if (tm.Unix()-periodShiftSec)%periodSec != 0 {
			continue
		}

		d.mu.RLock()
		for _, data := range d.jobDatas {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go d.loop(ctx, wg, tm, data)
		}
		d.mu.RUnlock()
	}
	wg.Wait()

	tm := time.Now()
	d.mu.RLock()
	for _, data := range d.jobDatas {
		wg.Add(1)
		go d.loop(ctx, wg, tm, data)
	}
	d.mu.RUnlock()
	wg.Wait()

	d.mu.Lock()
	d.finalize()
	d.mu.Unlock()

	eventLoopWg.Wait()
}

func (d *Daemon) initialize() {
	if d.eventsCh == nil {
		d.eventsCh = make(chan Event, 1024)
	}
	if d.jobDatas == nil {
		d.jobDatas = make(map[int]*JobData, 1024)
	}
}

func (d *Daemon) finalize() {
	close(d.eventsCh)
	d.eventsCh = nil
	d.jobDatas = nil
}

func (d *Daemon) getJobData(id int) *JobData {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.jobDatas[id]
}

func (d *Daemon) loop(ctx context.Context, wg *sync.WaitGroup, tm time.Time, data *JobData) {
	defer wg.Done()

	data.Lock()
	defer data.Unlock()

	if !data.InLoop && data.Err == nil && !data.Stopped {
		job := data.Job
		id := job.Id()
		data.InLoop = true
		data.Unlock()

		d.InfoLogger.Infof("job %d enters in loop", id)
		ok, e := job.Loop(ctx, tm)
		d.InfoLogger.Infof("job %d exits in loop", id)

		data.Lock()
		data.InLoop = false
		data.Stopped = !ok
		if e != nil {
			d.ErrorLogger.Errorf("job %d error: %v", id, e)
			data.Stopped = true
			data.Err = e
		}
		data.flush()
	}
}

func (d *Daemon) eventLoop(wg *sync.WaitGroup, eventsCh <-chan Event) {
	defer wg.Done()
	for event := range eventsCh {
		id := event.JobFrom.Id()
		data := d.getJobData(id)
		if data == nil {
			continue
		}
		data.Lock()
		for _, ch := range data.notificationEvents[event.Name] {
			if ch == nil {
				continue
			}
			select {
			case ch <- event:
			default:
				d.ErrorLogger.Errorf("event loop error for job %d and event %q: notification channel is full", id, event.Name)
			}
		}
		data.Unlock()
	}
}

func (d *Daemon) RegisterJob(job Job) error {
	id := job.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initialize()
	if _, ok := d.jobDatas[id]; ok {
		return newError(fmt.Errorf("%w: %d", ErrJobIdAlreadyRegistered, id))
	}
	if err := job.Init(d, func(eventName string, eventData interface{}) error {
		return d.sendEvent(job, eventName, eventData)
	}); err != nil {
		return newError(err)
	}
	d.jobDatas[id] = NewJobData(job)
	return nil
}

func (d *Daemon) UnregisterJob(job Job) error {
	id := job.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initialize()
	if _, ok := d.jobDatas[id]; ok {
		return newError(fmt.Errorf("%w: %d", ErrJobIdNotRegistered, id))
	}
	delete(d.jobDatas, id)
	return nil
}

func (d *Daemon) Jobs(includeStoppeds bool) ([]*JobData, error) {
	var err error
	d.mu.RLock()
	result := make([]*JobData, 0, len(d.jobDatas))
	for _, data := range d.jobDatas {
		if !includeStoppeds && data.Stopped {
			continue
		}
		result = append(result, data)
	}
	d.mu.RUnlock()
	for id, data := range result {
		result[id], err = data.DuplicateWithLock(nil)
		if err != nil {
			return nil, newError(err)
		}
	}
	return result, nil
}

func (d *Daemon) NotifyEvent(eventJobFrom Job, eventName string, ch chan<- Event) error {
	id := eventJobFrom.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	data := d.getJobData(id)
	if data == nil || eventJobFrom != data.Job {
		return newError(fmt.Errorf("%w: %d", ErrJobIdNotRegistered, id))
	}
	if ch == nil {
		return nil
	}
	data.Lock()
	defer data.Unlock()
	eventChs := data.notificationEvents[eventName]
	idx := sort.Search(len(eventChs), func(i int) bool {
		return eventChs[i] == ch
	})
	if len(eventChs) > 0 && idx >= 0 {
		return newError(ErrEventAlreadyRegistered)
	}
	if eventChs == nil {
		eventChs = make([]chan<- Event, 0, 16)
	}
	eventChs = append(eventChs, ch)
	data.notificationEvents[eventName] = eventChs
	return nil
}

func (d *Daemon) StopEventNotification(eventJobFrom Job, eventName string, ch chan<- Event) error {
	id := eventJobFrom.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	data := d.getJobData(id)
	if data == nil || eventJobFrom != data.Job {
		return newError(fmt.Errorf("%w: %d", ErrJobIdNotRegistered, id))
	}
	data.Lock()
	defer data.Unlock()
	if ch == nil {
		delete(data.notificationEvents, eventName)
		return nil
	}
	eventChs := data.notificationEvents[eventName]
	idx := sort.Search(len(eventChs), func(i int) bool {
		return eventChs[i] == ch
	})
	if len(eventChs) <= 0 || idx < 0 {
		return newError(ErrEventNotRegistered)
	}
	eventChs[idx] = nil
	idx = sort.Search(len(eventChs), func(i int) bool {
		return eventChs[i] == nil
	})
	if len(eventChs) > 0 && idx >= 0 {
		eventChs = eventChs[:idx]
	}
	if len(eventChs) <= 0 {
		delete(data.notificationEvents, eventName)
		return nil
	}
	data.notificationEvents[eventName] = eventChs
	return nil
}

func (d *Daemon) StopAllEventNotifications(eventJobFrom Job) error {
	id := eventJobFrom.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	data := d.getJobData(id)
	if data == nil || eventJobFrom != data.Job {
		return newError(fmt.Errorf("%w: %d", ErrJobIdNotRegistered, id))
	}
	data.Lock()
	defer data.Unlock()
	data.flush()
	return nil
}

func (d *Daemon) sendEvent(eventJobFrom Job, eventName string, eventData interface{}) error {
	id := eventJobFrom.Id()
	if id < 0 {
		return newError(fmt.Errorf("%w: %d", ErrInvalidJobId, id))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.initialize()
	data := d.jobDatas[id]
	if data == nil || eventJobFrom != data.Job {
		return newError(fmt.Errorf("%w: %d", ErrJobIdNotRegistered, id))
	}
	select {
	case d.eventsCh <- Event{
		JobFrom: eventJobFrom,
		Name:    eventName,
		Data:    eventData,
	}:
		return nil
	default:
		return newError(ErrEventBufferFull)
	}
}
