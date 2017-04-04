package server

import (
	"path"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// const (
// 	allocStep = uint64(1000)
// )

// // IDAllocator is the allocator to generate unique ID.
// type IDAllocator interface {
// 	Alloc() (uint64, error)
// }

type volumeIdAllocator struct {
	mu   sync.Mutex
	base uint64
	end  uint64
	s    *Server
}

func (alloc *volumeIdAllocator) Alloc() (uint64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.base == alloc.end {
		end, err := alloc.generate()
		if err != nil {
			return 0, errors.Trace(err)
		}

		alloc.end = end
		alloc.base = alloc.end - allocStep
	}
	alloc.base++
	return alloc.base, nil
}

func (alloc *volumeIdAllocator) generate() (uint64, error) {
	key := alloc.s.getVolumeAllocIDPath()
	value, err := getValue(alloc.s.client, key)
	log.Infof("Start Etcd Path:%s value:%s", key, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var (
		cmp clientv3.Cmp
		end uint64
	)
	if value == nil {
		// create the key
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		// update the key
		end, err = bytesToUint64(value)
		if err != nil {
			return 0, errors.Trace(err)
		}
		cmp = clientv3.Compare(clientv3.Value(key), "=", string(value))
	}
	end += allocStep
	value = uint64ToBytes(end)
	resp, err := alloc.s.leaderTxn(cmp).Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !resp.Succeeded {
		return 0, errors.New("generate id failed, we may not leader")
	}
	log.Infof("generate id:%d", end)
	return end, nil
}

func (s *Server) getVolumeAllocIDPath() string {
	return path.Join(s.rootPath, "alloc_volume_id")
}
