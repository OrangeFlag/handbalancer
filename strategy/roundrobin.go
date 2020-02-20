package strategy

import (
	"github.com/OrangeFlag/handbalancer/model"
	"github.com/afex/hystrix-go/hystrix"
	"sync"
	"sync/atomic"
)

type RoundRobinStrategy struct {
	pool         *model.Pool
	currentIndex uint64
	poolMutex    sync.RWMutex
}

func (r *RoundRobinStrategy) Next() *model.Worker {
	r.poolMutex.RLock()
	defer r.poolMutex.RUnlock()

	var worker *model.Worker
	for i := 0; i < r.pool.Len()*20; i++ {
		currentIndex := atomic.AddUint64(&r.currentIndex, 1)
		worker = (*r.pool)[currentIndex%uint64(r.pool.Len())]
		if circuit, _, _ := hystrix.GetCircuit(worker.Name); circuit.AllowRequest() {
			break
		}
	}
	return worker
}

func (r *RoundRobinStrategy) SetPool(pool *model.Pool) {
	r.poolMutex.Lock()
	defer r.poolMutex.Unlock()
	r.currentIndex = 0
	r.pool = pool
}
