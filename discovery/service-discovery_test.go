package discovery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"
)

var serviceName = "groupcache"

// Test with local consul
func _Test_SD(t *testing.T) {
	go runSD(context.Background(), "1", ":8081")
	go runSD(context.Background(), "2", ":8082")
	ctx, cancel := context.WithCancel(context.Background())
	go runSD(ctx, "3", ":8083")
	<-time.After(time.Second * 10)
	cancel()
	<-time.After(time.Minute)
}

type logger struct{}

func (l *logger) Errorf(msg string, args ...interface{}) {
	fmt.Printf(fmt.Sprintf(msg, args...) + "\n")
}

func runSD(ctx context.Context, id, addr string) {
	discovery, err := New("127.0.0.1:8500", serviceName, id, WithLogger(new(logger)), WithTTL(time.Second))
	check(err)

	runServer(ctx, addr)
	serviceAddr := fmt.Sprintf("http://localhost%s", addr)
	u, err := url.Parse(serviceAddr)
	check(err)
	err = discovery.Register(u, u)
	check(err)

	discovery.Watch(ctx, func(addr ...string) {
		watch(id, addr...)
	})
}

func watch(id string, addr ...string) error {
	fmt.Printf("ID: %s, AddrPool: %v\n", id, addr)
	return nil
}

func check(err error, msg ...string) {
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(fmt.Sprintf("Msg: %v, Error: %s", msg, err.Error()))
	}
}

func runServer(ctx context.Context, addr string) func() {
	s := http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("ok"))
		}),
	}

	c, cancel := context.WithCancel(ctx)
	go func() {
		err := s.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			check(err)
		}
	}()
	go func() {
		<-c.Done()
		err := s.Shutdown(ctx)
		check(err)
	}()

	return cancel
}
