package tso

import (
	"context"
	"log"
	"net"
	"sync/atomic"
	"time"

	pb "pebble-grpc/api" // Import your generated protobuf package

	"google.golang.org/grpc"
)

type TsoServer struct {
	pb.UnimplementedTSOServer
	counter int64
}

func (s *TsoServer) GetTS(ctx context.Context, in *pb.TSRequest) (*pb.TSResponse, error) {
	timestamp := time.Now().UnixNano()
	return &pb.TSResponse{Timestamp: timestamp}, nil
}

func (s *TsoServer) IncrementCounter() int64 {
	return atomic.AddInt64(&s.counter, 1)
}

func main() {
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterTSOServer(grpcServer, &TsoServer{})

	log.Println("TSO Server is running at :50052")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
