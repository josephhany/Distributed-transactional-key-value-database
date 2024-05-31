package main

import (
	"bytes"
	"context"
	"fmt"
	api "pebble-grpc/api"
	"strconv"
	"strings"
	"time"

	"github.com/linxGnu/grocksdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const timestampThreshold = 10 * time.Minute // Adjust the threshold as needed

type grpcServer struct {
	db             *grocksdb.TransactionDB
	columnFamilies map[string]*grocksdb.ColumnFamilyHandle
	api.UnimplementedKVServiceServer
}

func NewRPCServer(db *grocksdb.TransactionDB, columnFamilies map[string]*grocksdb.ColumnFamilyHandle) *grpc.Server {
	srv := grpcServer{
		db:             db,
		columnFamilies: columnFamilies,
	}

	gsrv := grpc.NewServer()
	api.RegisterKVServiceServer(gsrv, &srv)
	return gsrv
}

// prewrite << key, value, start_ts, PK >>
func (s *grpcServer) PreWrite(ctx context.Context, req *api.PreWriteRequest) (*api.PreWriteResponse, error) {

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	to := grocksdb.NewDefaultTransactionOptions()
	defer to.Destroy()

	// (1) Get handles for each column family
	writeCF, exists := s.columnFamilies["CF_WRITE"]
	if !exists {
		return nil, fmt.Errorf("write column family not found")
	}
	lockCF, exists := s.columnFamilies["CF_LOCK"]
	if !exists {
		return nil, fmt.Errorf("lock column family not found")
	}
	dataCF, exists := s.columnFamilies["default"]
	if !exists {
		return nil, fmt.Errorf("data/default column family not found")
	}

	// Start a transaction
	transaction := s.db.TransactionBegin(grocksdb.NewDefaultWriteOptions(), to, nil)
	if transaction == nil {
		to.Destroy()
		return nil, status.Errorf(codes.Internal, "Failed to start a transaction")
	}

	// Extract key and start timestamp as strings, then convert timestamp to int for comparison
	givenKey := req.GetKey()
	givenStartTs := req.GetStartTs()

	// Construct the key with format "Key_StartTS"
	foundKeyStartTs := fmt.Sprintf("%s_%d", givenKey, givenStartTs)
	PKStartTs := fmt.Sprintf("%s_%d", req.GetPrimaryKey(), givenStartTs)

	// (3) Check for write-write conflicts using a helper function
	if err := checkWriteWriteConflict(transaction, writeCF, givenKey, givenStartTs); err != nil {
		transaction.Rollback()
		return nil, err
	}

	// (4) Check for lock conflicts
	if err := checkLockConflict(transaction, lockCF, givenKey); err != nil {
		transaction.Rollback()
		return nil, err
	}

	// (5) Write the value for the key_start_ts in the data column family
	if err := transaction.PutCF(dataCF, []byte(foundKeyStartTs), req.GetValue()); err != nil {
		transaction.Rollback()
		return nil, status.Errorf(codes.Internal, "error setting key: %v", err)
	}

	// (6) Write the lock for the key_start_ts in the lock column family
	if err := transaction.PutCF(lockCF, []byte(PKStartTs), req.GetValue()); err != nil {
		transaction.Rollback()
		return nil, status.Errorf(codes.Internal, "error acquiring the lock: %v", err)
	}

	return &api.PreWriteResponse{}, nil
}

// Helper functions
func checkWriteWriteConflict(transaction *grocksdb.Transaction, cf *grocksdb.ColumnFamilyHandle, key []byte, startTs int64) error {
	readOpts := grocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetTotalOrderSeek(true)

	iter := transaction.NewIteratorCF(readOpts, cf)
	defer iter.Close()

	// Seek to the start key
	iter.Seek([]byte(fmt.Sprintf("%s_%d", key, startTs))) // ???
	for ; iter.Valid(); iter.Next() {
		foundKeyTs := iter.Key().Data()
		if strings.HasPrefix(string(foundKeyTs), string(key)) {
			parts := strings.Split(string(foundKeyTs), "_")
			if len(parts) > 1 {
				foundTs, err := strconv.ParseInt(parts[1], 10, 64)
				if err == nil && foundTs > startTs {
					return fmt.Errorf("write-write conflict detected with key %s at timestamp %d", foundKeyTs, foundTs)
				}
			}
		} else {
			break
		}
	}
	return nil
}

func checkLockConflict(transaction *grocksdb.Transaction, cf *grocksdb.ColumnFamilyHandle, key []byte) error {
	readOpts := grocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	iter := transaction.NewIteratorCF(readOpts, cf)
	defer iter.Close()

	iter.Seek(key)
	for ; iter.Valid(); iter.Next() {
		foundKey := iter.Key().Data()
		if strings.HasPrefix(string(foundKey), string(key)) {
			return fmt.Errorf("lock conflict detected with key %s", foundKey)
		} else {
			break
		}
	}
	return nil
}

// commit << key, commit_ts, start_ts, PK, isPK >>
func (s *grpcServer) Commit(ctx context.Context, req *api.CommitRequest) (*api.CommitResponse, error) {

	// Create options for the transaction and write
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	to := grocksdb.NewDefaultTransactionOptions()
	defer to.Destroy()

	// Get handles for each column family directly
	writeCF, exists := s.columnFamilies["CF_WRITE"]
	if !exists {
		return nil, fmt.Errorf("write column family not found")
	}
	lockCF, exists := s.columnFamilies["CF_LOCK"]
	if !exists {
		return nil, fmt.Errorf("lock column family not found")
	}

	// Start a transaction
	transaction := s.db.TransactionBegin(grocksdb.NewDefaultWriteOptions(), to, nil)
	if transaction == nil {
		to.Destroy()
		return nil, status.Errorf(codes.Internal, "Failed to start a transaction")
	}

	// Extract key and timestamps, convert timestamps to integers
	givenKey := req.GetKey()
	givenCommitTs := req.GetCommitTs()
	givenStartTs := req.GetStartTs()

	// Construct the keys
	foundKeyCommitTs := fmt.Sprintf("%s_%d", givenKey, givenCommitTs)
	PKStartTs := fmt.Sprintf("%s_%d", req.GetPrimaryKey(), givenStartTs)

	// If the commit is for the primary key
	if req.GetIsPrimary() {
		readOpts := grocksdb.NewDefaultReadOptions()
		defer readOpts.Destroy()

		iter := transaction.NewIteratorCF(readOpts, lockCF)
		defer iter.Close()

		iter.Seek([]byte(foundKeyCommitTs))

		// If the lock is not present, return an error
		if !iter.Valid() || string(iter.Key().Data()) != foundKeyCommitTs {
			transaction.Rollback()
			return nil, fmt.Errorf("lock lost detected")
		}
	}

	// Write commit data in the write column family
	data := fmt.Sprintf("Data@%d/%s", givenStartTs, PKStartTs)
	if err := transaction.PutCF(writeCF, []byte(foundKeyCommitTs), []byte(data)); err != nil {
		transaction.Rollback()
		return nil, fmt.Errorf("error writing commit data: %v", err)
	}

	// Clean the lock from the lock column family
	if err := transaction.DeleteCF(lockCF, []byte(fmt.Sprintf("%s_%d", givenKey, givenStartTs))); err != nil {
		transaction.Rollback()
		return nil, fmt.Errorf("error cleaning lock: %v", err)
	}

	// Commit the transaction
	if err := transaction.Commit(); err != nil {
		transaction.Rollback()
		return nil, status.Errorf(codes.Internal, "error committing transaction: %v", err)
	}

	return &api.CommitResponse{}, nil
}

// Given a key and a start timestamp, read the most recent value of the key
// that has a commit timestamp that is less than or equal to the start timestamp
func (s *grpcServer) readKey(ctx context.Context, req *api.ReadRequest) (*api.ReadResponse, error) {

	// (1) Get handles for each column family
	writeCF, exists := s.columnFamilies["CF_WRITE"]

	if !exists {
		return nil, fmt.Errorf("write column family not found")
	}
	lockCF, exists := s.columnFamilies["CF_LOCK"]
	if !exists {
		return nil, fmt.Errorf("lock column family not found")
	}
	dataCF, exists := s.columnFamilies["default"]
	if !exists {
		return nil, fmt.Errorf("data/default column family not found")
	}

	for {

		readOpts := grocksdb.NewDefaultReadOptions()
		defer readOpts.Destroy()
		readOpts.SetTotalOrderSeek(true) // Ensure the iterator examines all keys

		// (2) Create two iterators for the lock column family: one for the lock and one for the beginning
		lockIter := s.db.NewIteratorCF(readOpts, lockCF)
		defer lockIter.Close()

		begIter := s.db.NewIteratorCF(readOpts, lockCF)
		defer lockIter.Close()

		// (3) check for all locks CF here

		// create a key = key + "_" + startTS (note: we don't care who is the primary for this key)
		lockKey := fmt.Sprintf("%s_%d", req.GetKey(), req.GetStartTs())

		// Use Seek to find any existing locks for this key
		// Seek moves the iterator to the position greater than or equal to the key
		lockIter.Seek([]byte(lockKey))
		// We need to advance the iterator to the next key to accommodate for the case where lockIter = begIter
		lockIter.Next()

		// We use Seek(key) to get the iterator to the first key that is lexographically greater than
		// or equal to the "key"
		// We use Seek(key) instead of SeekToFirst() because we want don't want to start from the beginning of CF
		begIter.Seek(req.GetKey())

		reTry := false

		for begIter != lockIter {

			// Check if the key is a prefix of the lock CF key
			if bytes.HasPrefix(begIter.Key().Data(), req.GetKey()) {

				// If the lock is present and we have not re-tried, wait and retry
				if reTry == false {
					reTry = true
					time.Sleep(100 * time.Millisecond) // wait before retrying
					continue
				}

				// If we re-tried and the lock is still present, abort the reader
				return nil, fmt.Errorf("lock found for key %s", req.GetKey())
			}
			begIter.Next()
		}

		// (3) Create an iterator for the write column family
		iter := s.db.NewIteratorCF(readOpts, writeCF)
		defer iter.Close()

		// (4) Use SeekForPrev to find the highest timestamp less than or equal to startTS
		seekKey := fmt.Sprintf("%s_%d", req.GetKey(), req.GetStartTs())

		iter.SeekForPrev([]byte(seekKey))
		if !iter.Valid() {
			return nil, fmt.Errorf("no valid entry found")
		}

		// (5) Extract the PreWrite timestamp from the key
		PreWrite_TS := iter.Value().Data() // Assuming the timestamp is stored as the value
		iter.Close()

		// (6) Construct the PreWriteKey
		PreWriteKey := append(req.GetKey(), PreWrite_TS...)

		// (7) Since the lock is not present, read the actual data from the data column family (i.e., default) with the key_prewrite_ts
		dataValue, err := s.db.GetCF(readOpts, dataCF, PreWriteKey)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve data: %v", err)
		}
		defer dataValue.Free()

		// return string(dataValue.Data()), nil

		return &api.ReadResponse{
			Value:   dataValue.Data(),
			Success: true,
			Error:   "",
		}, nil

	}
}
