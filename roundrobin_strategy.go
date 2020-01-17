package lb

import (
	"github.com/afex/hystrix-go/hystrix"
	"sync"
	"sync/atomic"
)

type RoundRobinStrategy struct {
	pool         *Pool
	currentIndex uint64
	poolMutex    sync.RWMutex
}

func (r *RoundRobinStrategy) Next() *Worker {
	r.poolMutex.RLock()
	defer r.poolMutex.RUnlock()

	var worker *Worker
	for {
		currentIndex := atomic.AddUint64(&r.currentIndex, 1)
		worker = (*r.pool)[currentIndex%uint64(r.pool.Len())]
		if circuit, _, _ := hystrix.GetCircuit(worker.name); circuit.AllowRequest() {
			break
		}
	}
	return worker
}

func (r *RoundRobinStrategy) SetPool(pool *Pool) {
	r.poolMutex.Lock()
	defer r.poolMutex.Unlock()
	r.pool = pool;
}
