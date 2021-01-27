package daemon

type Event struct {
	JobFrom Job
	Name    string
	Data    interface{}
}
