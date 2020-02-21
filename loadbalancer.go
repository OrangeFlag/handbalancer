package handbalancer

import (
	"errors"
	"fmt"
	"github.com/OrangeFlag/handbalancer/model"
	"github.com/OrangeFlag/handbalancer/strategy"
	"github.com/afex/hystrix-go/hystrix"
	"strings"
	"sync"
	"sync/atomic"
)

type Balancer struct {
	name      string
	pool      model.Pool
	requests  chan *model.Request
	done      chan *model.Worker
	strategy  strategy.LoadBalancingStrategy
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

func NewBalancer(name string, strategy strategy.LoadBalancingStrategy) *Balancer {
	return NewBalancerL(name, strategy, 100)
}

func NewBalancerL(name string, strategy strategy.LoadBalancingStrategy, chanLen int) *Balancer {
	b := &Balancer{
		name:     name,
		pool:     make(model.Pool, 0),
		requests: make(chan *model.Request, chanLen),
		done:     make(chan *model.Worker, chanLen),
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

func (b *Balancer) AddPerformer(name string, performer func(interface{}) (interface{}, error), fallback func(error) error, circuitBreakerConfig *CircuitBreakerConfig) {
	b.AddPerformerW(name, performer, fallback, circuitBreakerConfig, 1)
}

func (b *Balancer) AddPerformerW(name string, performer func(interface{}) (interface{}, error), fallback func(error) error, circuitBreakerConfig *CircuitBreakerConfig, weight int) {
	b.poolMutex.Lock()
	defer b.poolMutex.Unlock()

	performerName := strings.Join([]string{b.name, name}, "-")

	if circuitBreakerConfig == nil {
		circuitBreakerConfig = &CircuitBreakerConfig{
			Timeout:                10000,
			MaxConcurrentRequests:  100,
			RequestVolumeThreshold: 5,
			SleepWindow:            5000,
			ErrorPercentThreshold:  25,
		}
	}
	hystrix.ConfigureCommand(performerName, hystrix.CommandConfig(*circuitBreakerConfig))
	pool := make([]*model.Worker, b.pool.Len()+weight)

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

	for i := b.pool.Len(); i < b.pool.Len()+weight; i++ {
		w := &model.Worker{
			Name:      performerName,
			Requests:  make(chan *model.Request, circuitBreakerConfig.MaxConcurrentRequests/weight*2),
			Performer: performerHystrix}
		pool[i] = w
		go w.Work(b.done)
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

func (b *Balancer) dispatch(req *model.Request) {
	w := b.strategy.Next()
	if w == nil {
		req.ErrorChan <- fmt.Errorf("received worker is nil")
	} else {
		w.Requests <- req
		atomic.AddUint64(&w.Pending, 1)
		//	fmt.Printf("started %p; now %d\n", w, w.Pending)
	}
}

func (b *Balancer) completed(w *model.Worker) {
	atomic.AddUint64(&w.Pending, ^uint64(0))
}

func Do(name string, run model.RunFunc, fallback model.FallbackFunc) (interface{}, error) {
	resultChan, errChan := Go(name, run, fallback)

	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errChan:
		return nil, err
	}
}

func Go(name string, run model.RunFunc, fallback model.FallbackFunc) (chan interface{}, chan error) {
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

	balancer.requests <- &model.Request{
		Func:       handler,
		ResultChan: resultChan,
		ErrorChan:  errChan,
	}

	return resultChan, errChan
}
