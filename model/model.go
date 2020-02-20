package model

type RunFunc func() (interface{}, error)
type FallbackFunc func(error) (interface{}, error)

type Request struct {
	Func       RunFunc
	ResultChan chan interface{}
	ErrorChan  chan error
}

type Worker struct {
	Name      string
	Idx       uint64
	Requests  chan *Request
	Pending   uint64
	Performer func(interface{}) (interface{}, error)
}

func (w *Worker) Work(done chan *Worker) {
	for {
		req := <-w.Requests
		if valueF, err := req.Func(); err == nil {
			if valueP, err := w.Performer(valueF); err == nil {
				req.ResultChan <- valueP
			} else {
				req.ErrorChan <- err
			}
		} else {
			req.ErrorChan <- err
		}
		done <- w
	}
}

type Pool []*Worker

func (p Pool) Len() int { return len(p) }
