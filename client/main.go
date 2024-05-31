package main

import (
	"context"
	"log"

	tso "pebble-grpc/TSO"
	api "pebble-grpc/api"

	gRPC "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr      = "localhost:50051"
	tsoAddr   = "localhost:50052"
	tsoClient *tso.TSO
)

type Transaction struct {
	client   api.KVServiceClient
	startTS  int64
	commitTS int64
	// orderedOps []Operation
	reads   []Read
	writes  []Write
	primary string
}

type Read struct {
	key string
}

type Write struct {
	key   string
	value string
}

// ReadAll
func (t *Transaction) ReadAll(reads []Read) {

	// (1) Get start timestamp from TSO
	t.startTS = tsoClient.GetTS()

	// (2) Read all keys

	for _, op := range reads {

		readRequest := &api.ReadRequest{
			Key:     []byte(op.key),
			StartTs: t.startTS,
		}
		_, err := t.client.Read(context.Background(), readRequest)

		if err != nil {
			log.Printf("error during read: %v", err)
			return
		}
	}

}

// All write keys, All values, start_ts
// inside we will know: PK, commit_ts, isPK

func (t *Transaction) CommitAll(writes []Write) {

	// (1) Choose the primary key to be the first key in the writes
	t.primary = writes[0].key

	// (2) Prewrite primary key first

	// Construct the prewrite request
	PreWriteRequest := &api.PreWriteRequest{
		Key:        []byte(writes[0].key),
		Value:      []byte(writes[0].value),
		StartTs:    t.startTS,
		PrimaryKey: []byte(t.primary),
	}

	// Send the prewrite request
	_, err := t.client.PreWrite(context.Background(), PreWriteRequest)

	if err != nil {
		log.Printf("error during primary key prewrite: %v", err)
		return
	}

	// (3) Prewrite the rest of the keys
	for _, op := range writes[1:] {
		PreWriteRequest := &api.PreWriteRequest{
			Key:        []byte(op.key),
			Value:      []byte(op.value),
			StartTs:    t.startTS,
			PrimaryKey: []byte(t.primary),
		}
		_, err := t.client.PreWrite(context.Background(), PreWriteRequest)

		if err != nil {
			log.Printf("error during prewrite: %v", err)
			return
		}
	}

	// (4) Get commit timestamp from TSO
	t.commitTS = tsoClient.GetTS()

	// Commit the primary key
	CommitRequest := &api.CommitRequest{
		Key:        []byte(t.primary),
		CommitTs:   t.commitTS,
		StartTs:    t.startTS,
		PrimaryKey: []byte(t.primary),
		IsPrimary:  true,
	}

	_, err = t.client.Commit(context.Background(), CommitRequest)

	if err != nil {
		log.Printf("error during primary key commit: %v", err)
		return
	}

	// Commit the rest of the keys
	for _, op := range writes[1:] {
		CommitRequest := &api.CommitRequest{
			Key:        []byte(op.key),
			CommitTs:   t.commitTS,
			StartTs:    t.startTS,
			PrimaryKey: []byte(t.primary),
			IsPrimary:  false,
		}

		_, err = t.client.Commit(context.Background(), CommitRequest)

		if err != nil {
			log.Printf("error during commit: %v", err)
			return
		}
	}

	log.Println("Transaction committed successfully")

}

func main() {

	// Connect to KV server
	conn, err := gRPC.Dial(addr, gRPC.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	client := api.NewKVServiceClient(conn)

	// Connect to TSO server
	tsoClient, err = tso.NewTSO(tsoAddr)
	if err != nil {
		log.Fatalf("Failed to connect to TSO server: %v", err)
	}

	// Create a transaction
	txn := &Transaction{
		client: client,
	}

	// Set the reads and writes
	txn.reads = []Read{{key: "key1"}, {key: "key2"}}
	txn.writes = []Write{{key: "key1", value: "value1"}, {key: "key2", value: "value2"}}

	// Read all keys
	txn.ReadAll(txn.reads)

	// Write all keys
	txn.CommitAll(txn.writes)

	log.Println("Transaction committed successfully")
}
