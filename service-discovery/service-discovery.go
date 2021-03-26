package service_discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/asim/go-micro/v3/registry"
	"github.com/asim/go-micro/v3/util/log"
)

type SD struct {
	//service micro.Service
	registry registry.Registry
}

type Action string

const (
	Delete Action = "delete"
	Create Action = "create"
	Update Action = "update"
)

type WatchFunc func(action Action, node []*registry.Node) error

func (s *SD) Watch(ctx context.Context, watchFunc WatchFunc) error {
	//watch, err := s.service.Options().Registry.Watch()
	watch, err := s.registry.Watch()
	if err != nil {
		return err
	}

	//TODO del
	go func() {
		<-ctx.Done()
		fmt.Println("stop watch")
		watch.Stop()
	}()

	go func() {
		for {
			next, err := watch.Next()
			if err != nil {
				log.Fatal(err)
				return
			}

			err = watchFunc(Action(next.Action), next.Service.Nodes)
			if err != nil {
				log.Fatal(err)
				return
			}
		}
	}()

	return nil
}

type DeregisterFunc func() error

func New(ctx context.Context, serviceName, nodeID, nodeAddr string) (*SD, error) {
	r := registry.NewRegistry()
	err := r.Register(&registry.Service{
		Name:    serviceName,
		Version: "0.1.0",
		Nodes: []*registry.Node{{
			Id:      nodeID,
			Address: nodeAddr,
		}},
	}, registry.RegisterTTL(time.Second*10))

	//service := micro.NewService(
	//	micro.Name(serviceName),
	//	micro.Auth(auth.NewAuth(auth.Namespace("namespace"))),
	//	micro.Context(ctx),
	//	micro.Address(nodeAddr),
	//	micro.RegisterTTL(time.Second*20),
	//	micro.RegisterInterval(time.Second*10),
	//)
	//
	//sd := &SD{service: service}
	//go func() {
	//	err := sd.service.Run()
	//	log.Fatal(err)
	//}()
	//
	////TODO del
	//go func() {
	//	<-ctx.Done()
	//	fmt.Printf("stop %s node\n", nodeID)
	//	sd.service.Server().Stop()
	//}()
	//
	//return sd, nil
	return &SD{registry: r}, err
}
