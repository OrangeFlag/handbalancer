handbalancer
==========

How to use
----------

```go
import (
    lb "github.com/OrangeFlag/handbalancer"
    "github.com/OrangeFlag/handbalancer/strategy"
)
```

### Create balancer
```go
var balancer = lb.NewBalancer("balancer_name", &strategy.RoundRobinStrategy{})
balancer.AddPerformer("performer_name", 
        func(i interface{}) (interface{}, error) {
		//some code for performer
	}, nil, nil)
```

### Execute code as a command

Define your application logic which relies on external systems, passing your function to ```lb.Go```. When that system is healthy this will be the only thing which executes.

```go
lb.Go("balancer_name", func() (interface{}, error) {
	// talk to other services
    result := nil
	return result, nil
}, nil)
```

### Defining fallback behavior for command

If you want code to execute during a service outage, pass in a second function to ```lb.Go```. Ideally, the logic here will allow your application to gracefully handle external services being unavailable.

```go
lb.Go("balancer_name", func() (interface{}, error) {
	// talk to other services
    result := nil
    return result, nil
}, func(error) (interface{}, error) {
    // try to fallback
    result := nil
    return result, nil
})
```

### Synchronous API

Since calling a command and immediately waiting for it to finish is a common pattern, a synchronous API is available with the `lb.Do` function.

```go
result, error := lb.Do("balancer_name", func() (interface{}, error) {
    // talk to other services
    result := nil
    return result, nil
}, nil)
```

### Configure settings

During adding performer you can configure circuit breaker

```go
balancer.AddPerformer("performer_name", 
    func(i interface{}) (interface{}, error) { 
        //some code for performer 
    }, nil, 
    &lb.CircuitBreakerConfig{
        	Timeout:                10000,
        	MaxConcurrentRequests:  100,
        	RequestVolumeThreshold: 5,
        	SleepWindow:            5000,
        	ErrorPercentThreshold:  25,
    })
```

### Enable dashboard metrics

> In the next releases
