package main

import (
	"flag"
	"log"
	"net"

	"github.com/linxGnu/grocksdb"
)

var (
	port = flag.String("port", "50051", "The server port")
)

func main() {
	flag.Parse()
	log.Println("Starting listening on port ", *port)

	listen, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Open RocksDB database with default options
	rocksOpts := grocksdb.NewDefaultOptions()
	rocksOpts.SetCreateIfMissing(true)

	// Transaction DB options
	transOpts := grocksdb.NewDefaultTransactionDBOptions()
	defer transOpts.Destroy()

	// Column family names
	cfNames := []string{"default", "CF_LOCK", "CF_WRITE"}
	var cfOptions []*grocksdb.Options

	// Create options for each column family
	for range cfNames {
		cfOption := grocksdb.NewDefaultOptions()
		cfOptions = append(cfOptions, cfOption)
	}

	// Attempt to open the transactional database with specified column families
	db, cfHandles, err := grocksdb.OpenTransactionDbColumnFamilies(rocksOpts, transOpts, "rocks_test_db", cfNames, cfOptions)
	if err != nil {
		log.Printf("Failed to open transactional database with column families: %v. Attempting to create column families individually.", err)

		// Open the transactional database with only the default column family initially
		db, err = grocksdb.OpenTransactionDb(rocksOpts, transOpts, "rocks_test_db")
		if err != nil {
			log.Fatalf("Failed to open transactional database: %v", err)
		}

		// Create missing column families one by one
		cfHandles = make([]*grocksdb.ColumnFamilyHandle, len(cfNames))
		for i, cfName := range cfNames {
			if cfName == "default" { // Skip creating the default column family as it's already created
				continue
			}
			cfHandle, err := db.CreateColumnFamily(rocksOpts, cfName)
			if err != nil {
				log.Fatalf("Failed to create column family '%s': %v", cfName, err)
			}
			cfHandles[i] = cfHandle
		}
	}
	defer db.Close()

	// Map column family handles
	columnFamilies := make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, name := range cfNames {
		columnFamilies[name] = cfHandles[i]
		defer cfHandles[i].Destroy()
	}

	// Start the server with the database and column family handles
	srv := NewRPCServer(db, columnFamilies)
	if err := srv.Serve(listen); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
