package tso

import (
	"context"
	"log"
	"time"

	pb "pebble-grpc/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TSO struct {
	client pb.TSOClient
}

func NewTSO(addr string) (*TSO, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewTSOClient(conn)
	return &TSO{client: client}, nil
}

func (t *TSO) GetTS() int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := t.client.GetTS(ctx, &pb.TSRequest{})
	if err != nil {
		log.Fatalf("Could not get timestamp: %v", err)
	}
	return resp.Timestamp
}
