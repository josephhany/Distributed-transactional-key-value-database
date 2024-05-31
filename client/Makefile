##
# gRPC-based Pebble K/V Service - Client Side
#
ROCKSDB_PATH=/home/jboulis/Documents/Research/code/rocksdb

CFLAGS="-I${ROCKSDB_PATH}/include"
LDFLAGS="-L${ROCKSDB_PATH} -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd -lbz2"

all:  client

client:  main.go
	CGO_CFLAGS=${CFLAGS} CGO_LDFLAGS=${LDFLAGS} go build -o client main.go

clean:
	/bin/rm -f client
# end
