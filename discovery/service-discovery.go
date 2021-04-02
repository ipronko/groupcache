package discovery

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

const (
	consulStatusPass = "passing"
)

type Logger interface {
	Errorf(template string, args ...interface{})
}

type nopLogger struct{}

func (l nopLogger) Errorf(_ string, _ ...interface{}) {}

type WatchFunc func(addr ...string) error

type Option func(sd *ServiceDiscovery)

func WithLogger(l Logger) Option {
	return func(sd *ServiceDiscovery) {
		sd.logger = l
	}
}

func WithTTL(duration time.Duration) Option {
	return func(sd *ServiceDiscovery) {
		sd.failTTL = duration
	}
}

func New(consulAddr, serviceName, serviceID string, opts ...Option) (*ServiceDiscovery, error) {
	client, err := api.NewClient(&api.Config{Address: consulAddr})
	if err != nil {
		return nil, errors.WithMessage(err, "create new consul client")
	}
	sd := &ServiceDiscovery{
		agent:       client.Agent(),
		serviceName: serviceName,
		serviceID:   serviceID,
		failTTL:     10 * time.Second,
		logger:      nopLogger{},
	}
	for _, opt := range opts {
		opt(sd)
	}
	return sd, nil
}

type ServiceDiscovery struct {
	agent       *api.Agent
	logger      Logger
	serviceName string
	serviceID   string
	failTTL     time.Duration
}

func (s *ServiceDiscovery) Register(serviceAddr, httpHealthAddr string) error {
	reg := &api.AgentServiceRegistration{
		Name:    s.serviceName,
		ID:      s.serviceID,
		Address: serviceAddr,
		Check: &api.AgentServiceCheck{
			HTTP:          httpHealthAddr,
			Interval:      s.failTTL.String(),
			TLSSkipVerify: true,
		},
	}

	return s.agent.ServiceRegister(reg)
}

func (s *ServiceDiscovery) Watch(ctx context.Context, watchFunc WatchFunc) error {
	t := time.NewTicker(time.Second)
	prevState := make([]string, 0)
	for {
		select {
		case <-ctx.Done():
			return s.agent.ServiceDeregister(s.serviceID)
		case <-t.C:
		}

		_, services, err := s.agent.AgentHealthServiceByName(s.serviceName)
		if err != nil {
			s.logger.Errorf("get list of services from consul err: %s", err.Error())
			continue
		}

		currState := make([]string, 0, len(services))
		for _, service := range services {
			if service.AggregatedStatus != consulStatusPass {
				continue
			}

			currState = append(currState, service.Service.Address)
		}

		sort.Strings(currState)

		if reflect.DeepEqual(prevState, currState) {
			continue
		}

		prevState = currState
		err = watchFunc(currState...)
		if err != nil {
			s.logger.Errorf("notify err: %s", err.Error())
		}
	}
}
