package strategy

import (
	"github.com/OrangeFlag/handbalancer/model"
	"sync"
	"sync/atomic"
)

type LeastConnStrategy struct {
	pool      *model.Pool
	poolMutex sync.RWMutex
	nextId    uint64
}

func (r *LeastConnStrategy) Next() *model.Worker {
	r.poolMutex.RLock()
	defer r.poolMutex.RUnlock()
	return (*r.pool)[atomic.LoadUint64(&r.nextId)]
}

var once sync.Once

func (r *LeastConnStrategy) SetPool(pool *model.Pool) {
	func() {
		r.poolMutex.Lock()
		defer r.poolMutex.Unlock()
		r.pool = pool
	}()
	once.Do(func() {
		go func() {
			r.poolMutex.RLock()
			defer r.poolMutex.RUnlock()
			if r.pool != nil && r.pool.Len() > 0 {
				min := (*r.pool)[atomic.LoadUint64(&r.nextId)].Pending
				for i := 0; i < r.pool.Len(); i++ {
					pending := (*r.pool)[i].Pending
					if pending < min {
						atomic.StoreUint64(&r.nextId, uint64(i))
					}
					min = (*r.pool)[atomic.LoadUint64(&r.nextId)].Pending
				}
			}
		}()
	})
}
