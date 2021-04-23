package discovery

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

type Logger interface {
	Errorf(template string, args ...interface{})
}

type nopLogger struct{}

func (l nopLogger) Errorf(_ string, _ ...interface{}) {}

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
		client:      client,
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
	client      *api.Client
	logger      Logger
	serviceName string
	serviceID   string
	failTTL     time.Duration
}

func (s *ServiceDiscovery) Register(serviceURL *url.URL, healthURL *url.URL) error {
	scheme, host, port, err := splitAddr(serviceURL)
	if err != nil {
		return fmt.Errorf("split addr err: %s", err)
	}

	reg := &api.AgentServiceRegistration{
		Name:    s.serviceName,
		ID:      s.serviceID,
		Address: fmt.Sprintf("%s://%s", scheme, host),
		Port:    port,
		Check: &api.AgentServiceCheck{
			HTTP:                           healthURL.String(),
			Interval:                       s.failTTL.String(),
			TLSSkipVerify:                  true,
			DeregisterCriticalServiceAfter: "1h",
		},
	}

	return s.client.Agent().ServiceRegister(reg)
}

func splitAddr(u *url.URL) (string, string, int, error) {
	if strings.Contains(u.Host, ":") {
		split := strings.Split(u.Host, ":")
		if len(split) != 2 {
			return "", "", 0, fmt.Errorf("splited host and port must contain two values")
		}
		host := split[0]
		portStr := split[1]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return "", "", 0, fmt.Errorf("parse int from service port")
		}
		return u.Scheme, host, port, nil
	}

	var port int
	if u.Scheme == "https" {
		port = 443
	}
	if u.Scheme == "http" {
		port = 80
	}
	return u.Scheme, u.Host, port, nil
}

func (s *ServiceDiscovery) Watch(ctx context.Context, watchFunc func(addr ...string)) error {
	t := time.NewTicker(time.Second)
	previousState := make([]string, 0)
	for {
		select {
		case <-ctx.Done():
			return s.client.Agent().ServiceDeregister(s.serviceID)
		case <-t.C:
		}

		services, _, err := s.client.Health().Service(s.serviceName, "", true, &api.QueryOptions{})
		if err != nil {
			s.logger.Errorf("get list of services from consul err: %s", err.Error())
			continue
		}

		currentState := make([]string, 0, len(services))
		for _, service := range services {
			currentState = append(currentState, fmt.Sprintf("%s:%d", service.Service.Address, service.Service.Port))
		}

		sort.Strings(currentState)

		if reflect.DeepEqual(previousState, currentState) {
			continue
		}

		previousState = currentState
		watchFunc(currentState...)
	}
}
