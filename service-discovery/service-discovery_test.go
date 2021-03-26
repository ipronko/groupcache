package service_discovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asim/go-micro/v3/registry"
	"github.com/asim/go-micro/v3/util/log"
)

var serviceName = "service"

func Test_SD(t *testing.T) {
	c, _ := context.WithTimeout(context.Background(), time.Second*10)
	runSD(context.Background(), "1", "8080")
	runSD(context.Background(), "2", "8081")
	runSD(c, "3", "8082")
	<-time.After(time.Minute)
}

func runSD(ctx context.Context, id, port string) {
	sd, err := New(ctx, serviceName, id, fmt.Sprintf("127.0.0.1:%s", port))
	if err != nil {
		log.Fatal("new registry: " + err.Error())
	}

	err = sd.Watch(ctx, func(action Action, node []*registry.Node) error {
		return watchFunc(id, action, node)
	})
	if err != nil {
		log.Fatal(err)
	}
}

func watchFunc(id string, action Action, node []*registry.Node) error {
	fmt.Printf("Server %s: GOT action: %s; nodeID: %s; nodeAddr: %s; node metadata: %v\n", id, action, node[0].Id, node[0].Address, node[0].Metadata)
	return nil
}
