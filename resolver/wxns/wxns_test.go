package wxns

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	_ "google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/structpb"
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

type GrpcTestServer struct {
	addr       string
	grpcServer *grpc.Server
	listener   net.Listener
	httpServer *http.Server
	wg         sync.WaitGroup
}

func NewGrpcTestServer(addr string) *GrpcTestServer {
	return &GrpcTestServer{
		addr: addr,
	}
}

func (s *GrpcTestServer) Start() error {
	// 启动 grpc
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis
	s.grpcServer = grpc.NewServer()

	s.grpcServer.RegisterService(serviceDesc, nil)
	reflection.Register(s.grpcServer)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("gRPC test server listening on %s", s.addr)
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("grpc server stopped: %v", err)
		}
	}()

	// 启动 http 服务（单独监听一个端口，这里举例 8080）
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc/meta", s.handleMeta)

	s.httpServer = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Println("HTTP server listening on :8080")
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server stopped: %v", err)
		}
	}()

	return nil
}

func (s *GrpcTestServer) handleMeta(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]interface{}{
		"service": "grpc test server",
		"version": "v1.0.0",
		"time":    time.Now().Format(time.RFC3339),
		"weight":  rand.Intn(40) + 80,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *GrpcTestServer) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("http server shutdown error: %v", err)
		}
	}
	s.wg.Wait()
	if s.listener != nil {
		s.listener.Close()
	}
}

var serviceDesc = &grpc.ServiceDesc{
	ServiceName: "test.TestService",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Test",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				return "hello from grpc-go server", nil
			},
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "test",
}

func loggingUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	p, ok := peer.FromContext(ctx)
	if ok {
		fmt.Printf("Calling %s on server address: %v\n", method, p.Addr)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func TestNewBuilder(t *testing.T) {
	// run server
	// handler := http.NewServeMux()
	// handler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	// 	log.Printf("%v\n", r.Host)
	// 	_, _ = w.Write([]byte("{\"weight\":90}"))
	// })
	// ports := []string{"20010"}
	// ms := NewMultiServer(ports, handler)
	// ms.Start()

	srv := NewGrpcTestServer(":20010")
	_ = srv.Start()

	// test code
	url := "wxns:///test.local:20010"
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"weighted_target_experimental":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(loggingUnaryInterceptor),
	}
	if conn, err := grpc.Dial(url, opts...); err == nil {
		conn.Connect()
		log.Printf("grpc status: %v", conn.GetState().String())

		method := "/echo.EchoService/Echo"
		request := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"message": structpb.NewStringValue("Hello, World!"),
			},
		}
		response := &structpb.Struct{}

		for range 1000 {
			err := conn.Invoke(context.Background(), method, request, response)
			log.Printf("invoke result: %v", err)
		}
	} else {
		log.Printf("grpc dial failed. err=%v", err)
	}

	// close server
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	// _ = ms.Stop(ctx)
	srv.Stop()
}
