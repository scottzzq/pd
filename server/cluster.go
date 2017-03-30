package server

import (
	"fmt"
	_ "math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	backgroundJobInterval = time.Minute
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex
	s           *Server
	running     bool
	clusterID   uint64
	clusterRoot string
	// cached cluster info
	cachedCluster *clusterInfo
	coordinator   *coordinator
	wg            sync.WaitGroup
	quit          chan struct{}
}

func newRaftCluster(s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()
	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}
	cluster, err := loadClusterInfo(c.s.idAlloc, c.s.kv)
	if err != nil {
		return errors.Trace(err)
	}
	if cluster == nil {
		return nil
	}
	c.cachedCluster = cluster

	c.coordinator = newCoordinator(c.cachedCluster, c.s.scheduleOpt)
	c.coordinator.run()

	c.wg.Add(1)
	c.quit = make(chan struct{})
	//go c.runBackgroundJobs(backgroundJobInterval)
	c.running = true
	return nil
}

func (c *RaftCluster) stop() {
	c.Lock()
	defer c.Unlock()
	if !c.running {
		return
	}
	c.running = false
	close(c.quit)
	c.wg.Wait()
	//c.coordinator.stop()
}

// func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
// 	defer c.wg.Done()
// 	ticker := time.NewTicker(interval)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-c.quit:
// 			return
// 		case <-ticker.C:
// 			c.checkStores()
// 			//c.collectMetrics()
// 		}
// 	}
// }

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) (*metapb.Store, *StoreStatus, error) {
	if storeID == 0 {
		return nil, nil, errors.New("invalid zero store id")
	}
	store := c.cachedCluster.getStore(storeID)
	if store == nil {
		return nil, nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}
	return store.Store, store.stats, nil
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *RaftCluster {
	if s.isClosed() || !s.cluster.isRunning() {
		return nil
	}
	return s.cluster
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}
	return s.cluster.start()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()
	return c.running
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	region := c.cachedCluster.getRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.Region, region.Leader
}

// func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
// 	storeMeta := req.GetStore()
// 	if storeMeta == nil {
// 		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
// 	} else if storeMeta.GetId() == 0 {
// 		return errors.New("invalid zero store id")
// 	}
// 	regionMeta := req.GetRegion()
// 	if regionMeta == nil {
// 		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
// 	} else if regionMeta.GetId() == 0 {
// 		return errors.New("invalid zero region id")
// 	}
// 	// } else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
// 	// 	// first region start/end key must be empty
// 	// 	return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
// 	// }

// 	peers := regionMeta.GetPeers()
// 	if len(peers) != 1 {
// 		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
// 	}
// 	peer := peers[0]
// 	if peer.GetStoreId() != storeMeta.GetId() {
// 		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
// 	}
// 	if peer.GetId() == 0 {
// 		return errors.New("invalid zero peer id")
// 	}
// 	return nil
// }

////////////////////////////////////////////初始化cluster
// message BootstrapRequest {
//     optional metapb.Store store   = 1;
//     optional metapb.Region region = 2;
// }
//enum StoreState {
//    Up        = 0;
//    Offline   = 1;
//    Tombstone = 2;
//}
//// Case insensitive key/value for replica constraints.
//message StoreLabel {
//    optional string key         = 1 [(gogoproto.nullable) = false];
//    optional string value       = 2 [(gogoproto.nullable) = false];
//}
//message Store {
//    optional uint64 id          = 1 [(gogoproto.nullable) = false];
//    optional string address     = 2 [(gogoproto.nullable) = false];
//    optional StoreState state   = 3 [(gogoproto.nullable) = false];
//    repeated StoreLabel labels  = 4;
//}
//message RegionEpoch {
//    // Conf change version, auto increment when add or remove peer
//    optional uint64 conf_ver	= 1 [(gogoproto.nullable) = false];
//    // Region version, auto increment when split or merge
//    optional uint64 version     = 2 [(gogoproto.nullable) = false];
//}
//message Region {
//    optional uint64 id                  = 1 [(gogoproto.nullable) = false];
//    // Region key range [start_key, end_key).
//    optional bytes  start_key           = 2;
//    optional bytes  end_key             = 3;
//    optional RegionEpoch region_epoch   = 4;
//    repeated Peer   peers               = 5;
//}
//message Peer {
//    optional uint64 id          = 1 [(gogoproto.nullable) = false];
//    optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
//}
//1、tikv的store分配Store id
//2、tikv的store分配Region id
//3、tikv的store判断是否已经初始化
//4、tikv的store判断未初始化需要发送bootstrapCluster指令
func (s *Server) BootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.Response, error) {
	clusterID := s.clusterID
	log.Infof("try to bootstrap raft cluster %d with %v", clusterID, req)
	//dev
	// if err := checkBootstrapRequest(clusterID, req); err != nil {
	// 	return nil, errors.Trace(err)
	// }
	// /pd/6396108630810176836/raft/
	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.cfg.Replication.MaxReplicas),
	}
	//1、Set cluster meta，设置集群信息
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterRootPath := s.getClusterRootPath()
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))
	log.Infof("bootstrapCluster, clusterRootPath:[%s] clusterValue:[%v]",
		clusterRootPath, clusterMeta)

	//2、Set store meta，设置Store meta信息， /pd/6396108630810176836/raft/s
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	//dev
	// //////////////////////////////////////////////store1
	// {
	// 	storeMeta := metapb.Store{
	// 		Id:      1,
	// 		Address: "127.0.0.1:20161",
	// 		State:   metapb.StoreState_Up,
	// 	}

	// 	//storeMeta := req.GetStore()
	// 	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	// 	storeValue, err := storeMeta.Marshal()
	// 	if err != nil {
	// 		return nil, errors.Trace(err)
	// 	}
	// 	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))
	// 	log.Infof("bootstrapCluster, storePath:[%s] storeValue:[%v]",
	// 		storePath, storeMeta)
	// }

	// //////////////////////////////////////////////store2
	// {
	// 	storeMeta := metapb.Store{
	// 		Id:      2,
	// 		Address: "127.0.0.1:20162",
	// 		State:   metapb.StoreState_Up,
	// 	}

	// 	//storeMeta := req.GetStore()
	// 	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	// 	storeValue, err := storeMeta.Marshal()
	// 	if err != nil {
	// 		return nil, errors.Trace(err)
	// 	}
	// 	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))
	// 	log.Infof("bootstrapCluster, storePath:[%s] storeValue:[%v]",
	// 		storePath, storeMeta)
	// }

	// //////////////////////////////////////////////store3
	// {
	// 	storeMeta := metapb.Store{
	// 		Id:      3,
	// 		Address: "127.0.0.1:20163",
	// 		State:   metapb.StoreState_Up,
	// 	}

	// 	//storeMeta := req.GetStore()
	// 	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	// 	storeValue, err := storeMeta.Marshal()
	// 	if err != nil {
	// 		return nil, errors.Trace(err)
	// 	}
	// 	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))
	// 	log.Infof("bootstrapCluster, storePath:[%s] storeValue:[%v]",
	// 		storePath, storeMeta)
	// }

	// 3、Set region meta with region id，设置Region信息, /pd/6396108630810176836/raft/r
	//dev
	// region := metapb.Region{
	// 	Id: 100,
	// }
	// region.RegionEpoch = &metapb.RegionEpoch{
	// 	ConfVer: 1,
	// 	Version: 1,
	// }
	// peers := make([]*metapb.Peer, 0, 3)
	// peers = append(peers, &metapb.Peer{Id: 4, StoreId: 1},
	// 	&metapb.Peer{Id: 5, StoreId: 2},
	// 	&metapb.Peer{Id: 6, StoreId: 3},
	// )
	// region.Peers = peers

	region := req.GetRegion()
	regionValue, err := region.Marshal()
	if err != nil {
		log.Errorf("bootstrapCluster region.Marsha error:%v", err)
		return nil, errors.Trace(err)
	}
	regionPath := makeRegionKey(clusterRootPath, region.GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))
	log.Infof("bootstrapCluster, regionPath:[%s] regionValue:[%v]",
		regionPath, region)

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := s.txn().If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Warnf("cluster %d already bootstrapped", clusterID)
		return newBootstrappedError(), nil
	}
	log.Infof("bootstrap cluster %d ok", clusterID)
	if err := s.cluster.start(); err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb.Response{
		Bootstrap: &pdpb.BootstrapResponse{},
	}, nil
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return strings.Join([]string{clusterRootPath, "s", fmt.Sprintf("%020d", storeID)}, "/")
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return strings.Join([]string{clusterRootPath, "r", fmt.Sprintf("%020d", regionID)}, "/")
}

//////////////////////////////////Store上报、、、、、、、、、、
func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()
	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}
	cluster := c.cachedCluster
	// Store address can not be the same as other stores.
	//同一个地址不允许启动两个store，但是如果老的下线之后是可以重新启动的
	for _, s := range cluster.getStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.isTombstone() {
			continue
		}
		if s.GetId() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.Store)
		}
	}
	//从现有的store中查找，没找到新增加一个，有的话，更新配置
	s := cluster.getStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = newStoreInfo(store)
	} else {
		// Update an existed store.
		s.Address = store.Address
		s.Labels = store.Labels
	}
	// Check location labels.
	for _, k := range c.s.cfg.Replication.LocationLabels {
		if v := s.getLabelValue(k); len(v) == 0 {
			return errors.Errorf("missing location label %q in store %v", k, s)
		}
	}
	return cluster.putStore(s)
}

///////////////////////Region心跳包、、、、、、、、、、、、、、、、
func (c *RaftCluster) handleRegionHeartbeat(region *regionInfo) (*pdpb.RegionHeartbeatResponse, error) {
	log.Info("RaftCluster handleRegionHeartbeat")
	// If the region peer count is 0, then we should not handle this.
	if len(region.GetPeers()) == 0 {
		log.Warnf("invalid region, zero region peer count - %v", region)
		return nil, errors.Errorf("invalid region, zero region peer count - %v", region)
	}
	return c.coordinator.dispatch(region), nil
}
