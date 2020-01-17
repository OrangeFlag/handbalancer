package lb

type LoadBalancingStrategy interface {
	Next() *Worker
	SetPool(pool *Pool)
}
