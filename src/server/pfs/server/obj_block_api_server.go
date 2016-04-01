package server

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"go.pedge.io/pb/go/google/protobuf"
	"go.pedge.io/proto/rpclog"
	"go.pedge.io/proto/stream"
	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
)

type objBlockAPIServer struct {
	protorpclog.Logger
	dir         string
	localServer *localBlockAPIServer
	objClient   obj.Client
}

func newObjBlockAPIServer(dir string, objClient obj.Client) (*objBlockAPIServer, error) {
	localServer, err := newLocalBlockAPIServer(dir)
	if err != nil {
		return nil, err
	}
	return &objBlockAPIServer{
		Logger:      protorpclog.NewLogger("pachyderm.pfsclient.objBlockAPIServer"),
		dir:         dir,
		localServer: localServer,
		objClient:   objClient,
	}, nil
}

func (s *objBlockAPIServer) PutBlock(putBlockServer pfsclient.BlockAPI_PutBlockServer) (retErr error) {
	result := &pfsclient.BlockRefs{}
	defer func(start time.Time) { s.Log(nil, result, retErr, time.Since(start)) }(time.Now())
	scanner := bufio.NewScanner(protostream.NewStreamingBytesReader(putBlockServer))
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for {
		blockRef, data, err := scanBlock(scanner)
		if err != nil {
			return err
		}
		result.BlockRef = append(result.BlockRef, blockRef)
		wg.Add(1)
		go func() {
			defer wg.Done()
			writer, err := s.objClient.Writer(s.localServer.blockPath(blockRef.Block))
			if err != nil {
				select {
				case errCh <- err:
					// error reported
				default:
					// not the first error
				}
				return
			}
			defer func() {
				if err := writer.Close(); err != nil {
					select {
					case errCh <- err:
						// error reported
					default:
						// not the first error
					}
					return
				}
			}()
			if _, err := writer.Write(data); err != nil {
				select {
				case errCh <- err:
					// error reported
				default:
					// not the first error
				}
				return
			}
		}()
		if (blockRef.Range.Upper - blockRef.Range.Lower) < uint64(blockSize) {
			break
		}
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	return putBlockServer.SendAndClose(result)
}

func (s *objBlockAPIServer) GetBlock(request *pfsclient.GetBlockRequest, getBlockServer pfsclient.BlockAPI_GetBlockServer) (retErr error) {
	defer func(start time.Time) { s.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	reader, err := s.objClient.Reader(s.localServer.blockPath(request.Block), request.OffsetBytes, request.SizeBytes)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	return protostream.WriteToStreamingBytesServer(reader, getBlockServer)
}

func (s *objBlockAPIServer) DeleteBlock(ctx context.Context, request *pfsclient.DeleteBlockRequest) (response *google_protobuf.Empty, retErr error) {
	defer func(start time.Time) { s.Log(request, response, retErr, time.Since(start)) }(time.Now())
	return google_protobuf.EmptyInstance, s.objClient.Delete(s.localServer.blockPath(request.Block))
}

func (s *objBlockAPIServer) InspectBlock(ctx context.Context, request *pfsclient.InspectBlockRequest) (response *pfsclient.BlockInfo, retErr error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *objBlockAPIServer) ListBlock(ctx context.Context, request *pfsclient.ListBlockRequest) (response *pfsclient.BlockInfos, retErr error) {
	defer func(start time.Time) { s.Log(request, response, retErr, time.Since(start)) }(time.Now())
	return nil, fmt.Errorf("not implemented")
}

func (s *objBlockAPIServer) CreateDiff(ctx context.Context, request *pfsclient.DiffInfo) (response *google_protobuf.Empty, retErr error) {
	defer func(start time.Time) { s.Log(request, response, retErr, time.Since(start)) }(time.Now())
	data, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	writer, err := s.objClient.Writer(s.localServer.diffPath(request.Diff))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := writer.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	return google_protobuf.EmptyInstance, nil
}

func (s *objBlockAPIServer) InspectDiff(ctx context.Context, request *pfsclient.InspectDiffRequest) (response *pfsclient.DiffInfo, retErr error) {
	defer func(start time.Time) { s.Log(request, response, retErr, time.Since(start)) }(time.Now())
	return s.readDiff(request.Diff)
}

func (s *objBlockAPIServer) ListDiff(request *pfsclient.ListDiffRequest, listDiffServer pfsclient.BlockAPI_ListDiffServer) (retErr error) {
	defer func(start time.Time) { s.Log(request, nil, retErr, time.Since(start)) }(time.Now())
	if err := s.objClient.Walk(s.localServer.diffDir(), func(path string) error {
		diff := s.localServer.pathToDiff(path)
		if diff == nil {
			return fmt.Errorf("couldn't parse %s", path)
		}
		if diff.Shard == request.Shard {
			diffInfo, err := s.readDiff(diff)
			if err != nil {
				return err
			}
			if err := listDiffServer.Send(diffInfo); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *objBlockAPIServer) DeleteDiff(ctx context.Context, request *pfsclient.DeleteDiffRequest) (response *google_protobuf.Empty, retErr error) {
	defer func(start time.Time) { s.Log(request, response, retErr, time.Since(start)) }(time.Now())
	return google_protobuf.EmptyInstance, s.objClient.Delete(s.localServer.diffPath(request.Diff))
}

func (s *objBlockAPIServer) readDiff(diff *pfsclient.Diff) (*pfsclient.DiffInfo, error) {
	reader, err := s.objClient.Reader(s.localServer.diffPath(diff), 0, 0)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	result := &pfsclient.DiffInfo{}
	if err := proto.Unmarshal(data, result); err != nil {
		return nil, err
	}
	return result, nil
}
