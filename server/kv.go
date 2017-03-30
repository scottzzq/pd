package server

import (
	"fmt"
	"math"
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"golang.org/x/net/context"
)

const (
	kvRangeLimit      = 100000
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

// kv wraps all kv operations, keep it stateless.
type kv struct {
	s           *Server
	client      *clientv3.Client
	clusterPath string
}

func newKV(s *Server) *kv {
	return &kv{
		s:      s,
		client: s.client,
		// /pd/6396108630810176836/raft
		clusterPath: path.Join(s.rootPath, "raft"),
	}
}

func (kv *kv) txn(cs ...clientv3.Cmp) clientv3.Txn { return kv.s.leaderTxn(cs...) }

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()
	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if cost := time.Since(start); cost > kvSlowRequestTime {
		log.Warnf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}

	return resp, errors.Trace(err)
}

func (kv *kv) load(key string) ([]byte, error) {
	resp, err := kvGet(kv.client, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return resp.Kvs[0].Value, nil
}

func (kv *kv) save(key, value string) error {
	resp, err := kv.txn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func (kv *kv) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	return true, proto.Unmarshal(value, msg)
}

func (kv *kv) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.save(key, string(value))
}

func (kv *kv) loadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(kv.clusterPath, meta)
}

func (kv *kv) saveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(kv.clusterPath, meta)
}

func (kv *kv) loadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return kv.loadProto(kv.storePath(storeID), store)
}

func (kv *kv) saveStore(store *metapb.Store) error {
	return kv.saveProto(kv.storePath(store.GetId()), store)
}

func (kv *kv) loadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.loadProto(kv.regionPath(regionID), region)
}

func (kv *kv) saveRegion(region *metapb.Region) error {
	return kv.saveProto(kv.regionPath(region.GetId()), region)
}

// enum StoreState {
//     Up        = 0;
//     Offline   = 1;
//     Tombstone = 2;
// }
// // Case insensitive key/value for replica constraints.
// message StoreLabel {
//     optional string key         = 1 [(gogoproto.nullable) = false];
//     optional string value       = 2 [(gogoproto.nullable) = false];
// }
// message Store {
//     optional uint64 id          = 1 [(gogoproto.nullable) = false];
//     optional string address     = 2 [(gogoproto.nullable) = false];
//     optional StoreState state   = 3 [(gogoproto.nullable) = false];
//     repeated StoreLabel labels  = 4;
//     // more attributes......
// }
func (kv *kv) loadStores(stores *storesInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endStore := kv.storePath(math.MaxUint64)
	withRange := clientv3.WithRange(endStore)
	withLimit := clientv3.WithLimit(rangeLimit)
	store_count := 0
	for {
		key := kv.storePath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range resp.Kvs {
			store := &metapb.Store{}
			if err := store.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}
			log.Infof("load Store:[%v]", store)
			nextID = store.GetId() + 1
			stores.setStore(newStoreInfo(store))
			store_count += 1
		}
		if len(resp.Kvs) < int(rangeLimit) {
			log.Infof("load total:[%d] stores!", store_count)
			return nil
		}
	}
	log.Infof("load total:[%d] stores!", store_count)
	return nil
}

// message RegionEpoch {
//     // Conf change version, auto increment when add or remove peer
//     optional uint64 conf_ver	= 1 [(gogoproto.nullable) = false];
//     // Region version, auto increment when split or merge
//     optional uint64 version     = 2 [(gogoproto.nullable) = false];
// }
// message Region {
//     optional uint64 id                  = 1 [(gogoproto.nullable) = false];
//     // Region key range [start_key, end_key).
//     optional bytes  start_key           = 2;
//     optional bytes  end_key             = 3;
//     optional RegionEpoch region_epoch   = 4;
//     repeated Peer   peers               = 5;
// }
// message Peer {
//     optional uint64 id          = 1 [(gogoproto.nullable) = false];
//     optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
// }
func (kv *kv) loadRegions(regions *regionsInfo, rangeLimit int64) error {
	nextID := uint64(0)
	endRegion := kv.regionPath(math.MaxUint64)
	withRange := clientv3.WithRange(endRegion)
	withLimit := clientv3.WithLimit(rangeLimit)
	region_count := 0
	for {
		key := kv.regionPath(nextID)
		resp, err := kvGet(kv.client, key, withRange, withLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, item := range resp.Kvs {
			region := &metapb.Region{}
			if err := region.Unmarshal(item.Value); err != nil {
				return errors.Trace(err)
			}
			log.Infof("load Region:[%v]", region)

			nextID = region.GetId() + 1
			regions.setRegion(newRegionInfo(region, nil))
			region_count += 1
		}

		if len(resp.Kvs) < int(rangeLimit) {
			log.Infof("load total:[%d] regions!", region_count)
			return nil
		}
	}
	log.Infof("load total:[%d] regions!", region_count)
	return nil
}

// /pd/6396108630810176836/raft/s00000000000001
// /pd/6396108630810176836/raft/s00000000000002
func (kv *kv) storePath(storeID uint64) string {
	return path.Join(kv.clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

// /pd/6396108630810176836/raft/r00000000000001
// /pd/6396108630810176836/raft/r00000000000002
func (kv *kv) regionPath(regionID uint64) string {
	return path.Join(kv.clusterPath, "r", fmt.Sprintf("%020d", regionID))
}
