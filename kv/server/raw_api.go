package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modifyOps := []storage.Modify{}
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Cf:    req.Cf,
			Value: req.Value,
		},
	}
	modifyOps = append(modifyOps, modify)
	err := server.storage.Write(req.Context, modifyOps)
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, nil
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modifyOps := []storage.Modify{}
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	modifyOps = append(modifyOps, modify)
	err := server.storage.Write(req.Context, modifyOps)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	kvs := []*kvrpcpb.KvPair{}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	start := 1
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if start > int(req.Limit) {
			break
		}
		item := iter.Item()
		val, _ := item.ValueCopy(nil)
		kv := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		}
		kvs = append(kvs, kv)
		start++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
