package discovery

import (
	"context"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

type Action string

const (
	Delete Action = "delete"
	Create Action = "create"
)

const (
	consulStatusPass = "passing"
	consulStatusWarn = "warning"
)

type Logger interface {
	Errorf(template string, args ...interface{})
}

type nopLogger struct{}

func (l nopLogger) Errorf(_ string, _ ...interface{}) {}

type WatchFunc func(action Action, addr string) error

type Option func(sd *ServiceDiscovery)

func WithLogger(l Logger) Option {
	return func(sd *ServiceDiscovery) {
		sd.logger = l
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
}

func (s *ServiceDiscovery) Register(serviceAddr, httpHealthAddr string) error {
	reg := &api.AgentServiceRegistration{
		Name:    s.serviceName,
		ID:      s.serviceID,
		Tags:    []string{"bar", "baz"},
		Address: serviceAddr,
		Check: &api.AgentServiceCheck{
			HTTP:          httpHealthAddr,
			Interval:      "5s",
			TLSSkipVerify: true,
		},
	}

	return s.agent.ServiceRegister(reg)
}

func (s *ServiceDiscovery) Watch(ctx context.Context, watchFunc WatchFunc) error {
	lastState := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, services, err := s.agent.AgentHealthServiceByName(s.serviceName)
		if err != nil {
			s.logger.Errorf("get list of services from consul err: %s", err.Error())
			continue
		}

		currState := make(map[string]bool)
		for _, service := range services {
			if service.Service.ID == s.serviceID {
				continue
			}

			if service.AggregatedStatus == consulStatusPass {
				currState[service.Service.Address] = true
			}
		}

		err = s.compareAndNotify(currState, lastState, watchFunc)
		if err != nil {
			s.logger.Errorf("notify err: %s", err.Error())
			continue
		}
	}
}

func (s *ServiceDiscovery) compareAndNotify(currentState, lastState map[string]bool, watchFunc WatchFunc) error {
	toDelete := make([]string, 0)
	toAdd := make([]string, 0)

	for addr := range lastState {
		if currentState[addr] {
			continue
		}
		err := watchFunc(Delete, addr)
		if err != nil {
			return errors.WithMessage(err, "call watch func with Delete event")
		}
		toDelete = append(toDelete, addr)
	}

	for addr := range currentState {
		if lastState[addr] {
			continue
		}
		err := watchFunc(Create, addr)
		if err != nil {
			return errors.WithMessage(err, "call watch func with Create event")
		}
		toAdd = append(toAdd, addr)
	}

	for _, addr := range toDelete {
		delete(lastState, addr)
	}

	for _, addr := range toAdd {
		lastState[addr] = true
	}

	return nil
}
