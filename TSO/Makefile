##
# gRPC-based Pebble K/V Service - TSO Server and Client
#

ROCKSDB_PATH=/home/jboulis/Documents/Research/code/rocksdb

CFLAGS="-I${ROCKSDB_PATH}/include"
LDFLAGS="-L${ROCKSDB_PATH} -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd -lbz2"

all: server client

server: server.go
	CGO_CFLAGS=${CFLAGS} CGO_LDFLAGS=${LDFLAGS} go build -o server server.go

client: client.go
	CGO_CFLAGS=${CFLAGS} CGO_LDFLAGS=${LDFLAGS} go build -o client client.go

clean:
	/bin/rm -f server client
# end
