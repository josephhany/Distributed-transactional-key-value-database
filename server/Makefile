##
# gRPC-based Pebble K/V Service
#
ROCKSDB_PATH=/home/jboulis/Documents/Research/code/rocksdb

CFLAGS="-I${ROCKSDB_PATH}/include"
LDFLAGS="-L${ROCKSDB_PATH} -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd -lbz2"

all:  server

server: main.go server.go
	CGO_CFLAGS=${CFLAGS} CGO_LDFLAGS=${LDFLAGS} go build -o server main.go server.go

clean:
	/bin/rm -f server
# end
