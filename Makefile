##
# gRPC-based Pebble K/V Service
#

all:  api server client
.PHONY: api server client

api:
	cd api; make

server:
	cd server; make

client:
	cd client; make

clean:
	cd api; make clean
	cd server; make clean
	cd client; make clean

# end
