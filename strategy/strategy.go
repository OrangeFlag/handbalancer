package strategy

import "github.com/OrangeFlag/handbalancer/model"

type LoadBalancingStrategy interface {
	Next() *model.Worker
	SetPool(pool *model.Pool)
}
