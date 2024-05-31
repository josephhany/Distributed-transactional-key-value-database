# Description
Minimal RocksDB Server using grpc and grocksdb.

# Build
You will need to install golang first.

Then, clone [rocksdb](https://github.com/facebook/rocksdb), checkout v8.11.3, and build
it using the [installation instructions](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)

In the pebble-grpc repo, you need to edit ROCKSDB_PATH in the Makefiles in the client and server directories
to point to your RocksDB installation.

Finally
``` sh
$ go mod init pebble-grpc
$ make
```
in the top directory should compile the interface, and build the client and server.
You will probably need to `go get...` some modules and then rebuild, on your first attempt.

# Run
To launch the server, go to the server directory and run

``` sh
$ ./server
```

To launch the client, go to the client directory and run

``` sh
$ ./client
```

Client talks to server via `localhost`, using a port that is hardcoded into client and server.
