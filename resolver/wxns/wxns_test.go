package wxns

import (
	"context"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	_ "google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type MultiServer struct {
	servers []*http.Server
	wg      sync.WaitGroup
}

func NewMultiServer(ports []string, handler http.Handler) *MultiServer {
	ms := &MultiServer{}
	for _, port := range ports {
		srv := &http.Server{
			Addr:    ":" + port,
			Handler: handler,
		}
		ms.servers = append(ms.servers, srv)
	}
	return ms
}

func (ms *MultiServer) Start() {
	for _, srv := range ms.servers {
		ms.wg.Add(1)
		go func(s *http.Server) {
			defer ms.wg.Done()
			log.Printf("Listening on %s", s.Addr)
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Server on %s exited with error: %v", s.Addr, err)
			}
		}(srv)
	}
}

func (ms *MultiServer) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, srv := range ms.servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			log.Printf("Shutting down server on %s", s.Addr)
			if err := s.Shutdown(ctx); err != nil {
				log.Printf("Error shutting down server on %s: %v", s.Addr, err)
			}
		}(srv)
	}
	wg.Wait()
	return nil
}

func TestNewBuilder(t *testing.T) {
	// run server
	handler := http.NewServeMux()
	handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%v\n", r.Host)
		_, _ = w.Write([]byte("{\"weight\":90}"))
	})

	ports := []string{"20010"}
	ms := NewMultiServer(ports, handler)
	ms.Start()

	// test code
	// url := "wxns:///baidu.com:20010"
	url := "wxns:///localhost:20010"
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"weighted_target_experimental":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if conn, err := grpc.Dial(url, opts...); err == nil {
		conn.Connect()
		method := "/echo.EchoService/Echo"
		request := struct{ Message string }{"Hello, World!"}
		var response struct{ Response string }

		conn.Invoke(context.Background(), method, &request, &response)
		log.Printf("grpc status: %v", conn.GetState().String())
	} else {
		log.Printf("grpc dial failed. err=%v", err)
	}

	// close server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ms.Stop(ctx)
}
