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

	"github.com/soheilhy/cmux"
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
	stopCh     chan struct{}
}

func NewGrpcTestServer(addr string) *GrpcTestServer {
	return &GrpcTestServer{
		addr:   addr,
		stopCh: make(chan struct{}),
	}
}

func (s *GrpcTestServer) Start() error {
	// 监听 TCP 端口
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	// 用 cmux 做协议复用
	m := cmux.New(lis)

	// 匹配 gRPC 请求 (HTTP/2 with content-type "application/grpc")
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	// 匹配其他所有请求，做 HTTP 服务器
	httpL := m.Match(cmux.Any())

	// 初始化 grpc.Server
	s.grpcServer = grpc.NewServer()
	// TODO: 这里注册你的 grpc 服务
	s.grpcServer.RegisterService(serviceDesc, nil)
	reflection.Register(s.grpcServer)

	// 初始化 http.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc/meta", s.handleMeta)
	s.httpServer = &http.Server{
		Handler: mux,
	}

	// 启动 grpc 服务 goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("gRPC server listening on %s", s.addr)
		if err := s.grpcServer.Serve(grpcL); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	// 启动 http 服务 goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		log.Printf("HTTP server listening on %s", s.addr)
		if err := s.httpServer.Serve(httpL); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server stopped: %v", err)
		}
	}()

	// 启动 cmux，阻塞直到关闭
	return m.Serve()
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
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

var serviceDesc = &grpc.ServiceDesc{
	ServiceName: "rpc",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "echo",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				p, ok := peer.FromContext(ctx)
				if ok {
					log.Printf("local IP localAddr: %v\n", p.LocalAddr)
					// log.Printf("client IP: %v", p.Addr)
				} else {
					log.Printf("local IP not found\n")
					// log.Printf("localAddr: %v", p.LocalAddr)
				}
				resp := map[string]interface{}{
					"weight": rand.Intn(40) + 80,
				}
				return json.Marshal(resp)
			},
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "test",
}

func loggingUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if p, ok := peer.FromContext(ctx); ok {
		log.Printf("invoked method=%s, remote addr=%v, err=%v\n", method, p.Addr, err)
	} else {
		log.Printf("invoked method=%s, remote addr unknown, err=%v\n", method, err)
	}
	return err
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
		// grpc.WithUnaryInterceptor(loggingUnaryInterceptor),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"weighted_round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if conn, err := grpc.Dial(url, opts...); err == nil {
		conn.Connect()
		log.Printf("grpc status: %v", conn.GetState().String())

		method := "/rpc/echo"
		request := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"message": structpb.NewStringValue("Hello, World!"),
			},
		}
		response := &structpb.Struct{}

		// time.Sleep(1 * time.Second)
		for range 10 {
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
