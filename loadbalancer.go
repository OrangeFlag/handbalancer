package lb

import (
	"errors"
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	"strings"
	"sync"
	"sync/atomic"
)

type runFunc func() (interface{}, error)
type fallbackFunc func(error) (interface{}, error)

type Balancer struct {
	name      string
	pool      Pool
	requests  chan *Request
	done      chan *Worker
	strategy  LoadBalancingStrategy
	poolMutex sync.Mutex
}

var (
	balancersMutex *sync.RWMutex
	balancers      map[string]*Balancer
)

func init() {
	balancersMutex = &sync.RWMutex{}
	balancers = make(map[string]*Balancer)
}

func NewBalancer(name string, strategy LoadBalancingStrategy) *Balancer {
	b := &Balancer{
		name:     name,
		pool:     make(Pool, 0),
		requests: make(chan *Request),
		done:     make(chan *Worker),
		strategy: strategy,
	}
	b.balance()
	return b
}

func AddBalancer(balancer *Balancer) error {
	balancersMutex.Lock()
	defer balancersMutex.Unlock()
	balancers[balancer.name] = balancer
	return nil
}

func GetBalancer(name string) (*Balancer, error) {
	balancersMutex.RLock()
	defer balancersMutex.RUnlock()
	if value, ok := balancers[name]; ok {
		return value, nil
	} else {
		return nil, errors.New(fmt.Sprintf("there is no balancer with name: %s", name))
	}
}

type Worker struct {
	name      string
	idx       uint64
	requests  chan *Request
	pending   uint64
	performer func(interface{}) (interface{}, error)
}

type Request struct {
	Func       runFunc
	ResultChan chan interface{}
	ErrorChan  chan error
}

func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests
		if valueF, err := req.Func(); err == nil {
			if valueP, err := w.performer(valueF); err == nil {
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

type CircuitBreakerConfig struct {
	// Timeout is how long to wait for command to bitcoinMininode, in milliseconds
	Timeout int
	// MaxConcurrentRequests is how many commands of the same type can run at the same time
	MaxConcurrentRequests int
	// RequestVolumeThreshold is the minimum number of requests needed before a circuit can be tripped due to health
	RequestVolumeThreshold int
	// SleepWindow is how long, in milliseconds, to wait after a circuit opens before testing for recovery
	SleepWindow int
	// ErrorPercentThreshold causes circuits to open once the rolling measure of errors exceeds this percent of requests
	ErrorPercentThreshold int
}

func (b *Balancer) AddPerformer(name string, performer func(interface{}) (interface{}, error)) {
	b.AddPerformerFCN(name, performer, nil, nil, 1)
}

func (b *Balancer) AddPerformerC(name string, performer func(interface{}) (interface{}, error), circuitBreakerConfig *CircuitBreakerConfig) {
	b.AddPerformerFCN(name, performer, nil, circuitBreakerConfig, 1)
}

func (b *Balancer) AddPerformerFC(name string, performer func(interface{}) (interface{}, error), fallback func(error) error, circuitBreakerConfig *CircuitBreakerConfig) {
	b.AddPerformerFCN(name, performer, fallback, circuitBreakerConfig, 1)
}

func (b *Balancer) AddPerformerFCN(name string, performer func(interface{}) (interface{}, error), fallback func(error) error, circuitBreakerConfig *CircuitBreakerConfig, count int) {
	b.poolMutex.Lock()
	defer b.poolMutex.Unlock()

	performerName := strings.Join([]string{b.name, name}, "-")

	if circuitBreakerConfig != nil {
		hystrix.ConfigureCommand(performerName, hystrix.CommandConfig(*circuitBreakerConfig))
	} else {
		hystrix.ConfigureCommand(performerName, hystrix.CommandConfig{
			Timeout:                10000,
			MaxConcurrentRequests:  100,
			RequestVolumeThreshold: 5,
			SleepWindow:            5000,
			ErrorPercentThreshold:  25,
		})
	}
	pool := make([]*Worker, b.pool.Len()+count)

	copy(pool, b.pool)

	performerHystrix := func(input interface{}) (interface{}, error) {
		output := make(chan interface{}, 1)
		errs := hystrix.Go(performerName, func() error {

			if ret, err := performer(input); err != nil {
				return err
			} else {
				output <- ret
			}
			return nil
		}, fallback)

		select {
		case out := <-output:
			return out, nil
		case err := <-errs:
			return nil, err
		}
	}

	for i := b.pool.Len(); i < b.pool.Len()+count; i++ {
		w := &Worker{
			name:      performerName,
			requests:  make(chan *Request),
			performer: performerHystrix}
		pool[i] = w
		go w.work(b.done)
	}

	b.pool = pool
	b.strategy.SetPool(&b.pool)
}

func (b *Balancer) balanceDispatch() {
	for {
		select {
		case req := <-b.requests:
			b.dispatch(req)
		}
	}
}

func (b *Balancer) balanceCompleted() {
	for {
		select {
		case w := <-b.done:
			b.completed(w)
		}
	}
}

func (b *Balancer) balance() {
	go b.balanceDispatch()
	go b.balanceCompleted()
}

//func (b *Balancer) print() {
//	sum := 0
//	sumsq := 0
//	for _, w := range b.pool {
//		fmt.Printf("%d ", w.pending)
//		sum += w.pending
//		sumsq += w.pending * w.pending
//	}
//	avg := float64(sum) / float64(len(b.pool))
//	variance := float64(sumsq)/float64(len(b.pool)) - avg*avg
//	fmt.Printf(" %.2f %.2f\n", avg, variance)
//}

func (b *Balancer) dispatch(req *Request) {
	w := b.strategy.Next()
	if w == nil {
		req.ErrorChan <- fmt.Errorf("received worker is nil")
	} else {
		w.requests <- req
		atomic.AddUint64(&w.pending, 1)
		//	fmt.Printf("started %p; now %d\n", w, w.pending)
	}
}

func (b *Balancer) completed(w *Worker) {
	atomic.AddUint64(&w.pending, ^uint64(0))
}

func Do(name string, run runFunc) (interface{}, error) {
	return doF(name, run, nil)
}

func doF(name string, run runFunc, fallback fallbackFunc) (interface{}, error) {
	resultChan, errChan := goF(name, run, fallback)

	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errChan:
		return nil, err
	}
}

func Go(name string, run runFunc) (chan interface{}, chan error) {
	return goF(name, run, nil)
}

func goF(name string, run runFunc, fallback fallbackFunc) (chan interface{}, chan error) {
	balancer, err := GetBalancer(name)

	errChan := make(chan error, 3)

	if err != nil {
		errChan <- err
		return nil, errChan
	}

	handler := func() (interface{}, error) {
		value, err := run()
		if err != nil {
			if fallback != nil {
				if value, err = fallback(err); err != nil {
					return nil, err
				} else {
					return value, nil
				}
			} else {
				return nil, err
			}
		}
		return value, nil
	}

	resultChan := make(chan interface{})

	balancer.requests <- &Request{
		Func:       handler,
		ResultChan: resultChan,
		ErrorChan:  errChan,
	}

	return resultChan, errChan
}
