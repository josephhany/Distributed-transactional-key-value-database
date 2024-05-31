##
# gRPC-based Pebble K/V Service
#

all:  pebblekv_grpc.pb.go pebblekv.pb.go

pebblekv_grpc.pb.go pebblekv.pb.go &: pebblekv.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pebblekv.proto

clean:
	/bin/rm -f pebblekv_grpc.pb.go pebblekv.pb.go
# end
