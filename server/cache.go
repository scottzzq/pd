package server

import (
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errStoreNotFound = func(storeID uint64) error {
		return errors.Errorf("store %v not found", storeID)
	}
	errStoreIsBlocked = func(storeID uint64) error {
		return errors.Errorf("store %v is blocked", storeID)
	}
	errRegionNotFound = func(regionID uint64) error {
		return errors.Errorf("region %v not found", regionID)
	}
	errRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
		return errors.Errorf("region is stale: region %v origin %v", region, origin)
	}
)

////////////////Cluster信息
type clusterInfo struct {
	sync.RWMutex
	id      IDAllocator
	kv      *kv
	meta    *metapb.Cluster
	stores  *storesInfo
	regions *regionsInfo
}

func newClusterInfo(id IDAllocator) *clusterInfo {
	return &clusterInfo{
		id:      id,
		stores:  newStoresInfo(),
		regions: newRegionsInfo(),
	}
}

func (c *clusterInfo) getStore(storeID uint64) *storeInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStore(storeID)
}

func (c *clusterInfo) getStores() []*storeInfo {
	c.RLock()
	defer c.RUnlock()
	return c.stores.getStores()
}

func (c *clusterInfo) putStore(store *storeInfo) error {
	c.Lock()
	defer c.Unlock()
	if c.kv != nil {
		if err := c.kv.saveStore(store.Store); err != nil {
			return errors.Trace(err)
		}
	}
	c.stores.setStore(store)
	return nil
}

func (c *clusterInfo) getRegion(regionID uint64) *regionInfo {
	c.RLock()
	defer c.RUnlock()
	return c.regions.getRegion(regionID)
}

func (c *clusterInfo) putRegion(region *regionInfo) error {
	c.Lock()
	defer c.Unlock()
	return c.putRegionLocked(region.clone())
}

func (c *clusterInfo) putRegionLocked(region *regionInfo) error {
	if c.kv != nil {
		if err := c.kv.saveRegion(region.Region); err != nil {
			return errors.Trace(err)
		}
	}
	c.regions.setRegion(region)
	return nil
}

/////////////////////Store信息、、、、、、、、、、
//key是store_id,value是storeInfo
type storesInfo struct {
	stores map[uint64]*storeInfo
}

func newStoresInfo() *storesInfo {
	return &storesInfo{
		stores: make(map[uint64]*storeInfo),
	}
}

func (s *storesInfo) getStore(storeID uint64) *storeInfo {
	store, ok := s.stores[storeID]
	if !ok {
		return nil
	}
	return store.clone()
}

func (s *storesInfo) setStore(store *storeInfo) {
	s.stores[store.GetId()] = store
}

func (s *storesInfo) getStores() []*storeInfo {
	stores := make([]*storeInfo, 0, len(s.stores))
	for _, store := range s.stores {
		stores = append(stores, store.clone())
	}
	return stores
}

/////////////////////Region信息、、、、、、、、、、
type regionsInfo struct {
	//tree      *regionTree
	regions   map[uint64]*regionInfo            // regionID -> regionInfo
	leaders   map[uint64]map[uint64]*regionInfo // storeID -> regionID -> regionInfo
	followers map[uint64]map[uint64]*regionInfo // storeID -> regionID -> regionInfo
}

func newRegionsInfo() *regionsInfo {
	return &regionsInfo{
		//tree:      newRegionTree(),
		regions:   make(map[uint64]*regionInfo),
		leaders:   make(map[uint64]map[uint64]*regionInfo),
		followers: make(map[uint64]map[uint64]*regionInfo),
	}
}

func (r *regionsInfo) getRegion(regionID uint64) *regionInfo {
	region, ok := r.regions[regionID]
	if !ok {
		return nil
	}
	return region.clone()
}

func (r *regionsInfo) setRegion(region *regionInfo) {
	if origin, ok := r.regions[region.GetId()]; ok {
		r.removeRegion(origin)
	}
	r.addRegion(region)
}

func (r *regionsInfo) addRegion(region *regionInfo) {
	// Add to tree and regions.
	//r.tree.update(region.Region)
	r.regions[region.GetId()] = region
	if region.Leader == nil {
		return
	}
	// Add to leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.Leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = make(map[uint64]*regionInfo)
				r.leaders[storeID] = store
			}
			store[region.GetId()] = region
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = make(map[uint64]*regionInfo)
				r.followers[storeID] = store
			}
			store[region.GetId()] = region
		}
	}
}

func (r *regionsInfo) removeRegion(region *regionInfo) {
	// Remove from tree and regions.
	//r.tree.remove(region.Region)
	delete(r.regions, region.GetId())

	// Remove from leaders and followers.
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		delete(r.leaders[storeID], region.GetId())
		delete(r.followers[storeID], region.GetId())
	}

}
func (r *regionsInfo) getStoreLeaderCount(storeID uint64) int {
	return len(r.leaders[storeID])
}

// Return nil if cluster is not bootstrapped.
func loadClusterInfo(id IDAllocator, kv *kv) (*clusterInfo, error) {
	c := newClusterInfo(id)
	c.kv = kv
	// message Cluster {
	//    	optional uint64 id              = 1 [(gogoproto.nullable) = false];
	//    	// max peer count for a region.
	//    	// pd will do the auto-balance if region peer count mismatches.
	//    	optional uint32 max_peer_count  = 2 [(gogoproto.nullable) = false];
	//    	// more attributes......
	// }
	c.meta = &metapb.Cluster{}
	//1、加载meta信息
	ok, err := kv.loadMeta(c.meta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("pd load ClusterInfo:[%v]", c.meta)
	if !ok {
		return nil, nil
	}
	start := time.Now()
	//2、加载所有的Stores, 100000
	if err := kv.loadStores(c.stores, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v stores cost %v", c.stores.getStoreCount(), time.Since(start))
	start = time.Now()
	//3、加载所有的Regions, 100000
	if err := kv.loadRegions(c.regions, kvRangeLimit); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("load %v regions cost %v", c.regions.getRegionCount(), time.Since(start))
	return c, nil
}

func (s *storesInfo) getStoreCount() int {
	return len(s.stores)
}

func (r *regionsInfo) getRegionCount() int {
	return len(r.regions)
}

////////////////////处理Store心跳包
// handleStoreHeartbeat updates the store status.
func (c *clusterInfo) handleStoreHeartbeat(stats *pdpb.StoreStats) error {
	c.Lock()
	defer c.Unlock()

	storeID := stats.GetStoreId()
	store := c.stores.getStore(storeID)
	if store == nil {
		return errors.Trace(errStoreNotFound(storeID))
	}
	store.stats.StoreStats = proto.Clone(stats).(*pdpb.StoreStats)
	store.stats.LeaderCount = uint32(c.regions.getStoreLeaderCount(storeID))
	store.stats.LastHeartbeatTS = time.Now()
	c.stores.setStore(store)
	return nil
}

/////////////////////处理Region心跳包、、、、、、、、、、
// handleRegionHeartbeat updates the region information.
func (c *clusterInfo) handleRegionHeartbeat(region *regionInfo) error {
	c.Lock()
	defer c.Unlock()
	region = region.clone()
	origin := c.regions.getRegion(region.GetId())
	// Region does not exist, add it.
	if origin == nil {
		log.Infof("[region %d] Insert new region {%v}", region.GetId(), region)
		return c.putRegionLocked(region)
	}
	r := region.GetRegionEpoch()
	o := origin.GetRegionEpoch()
	// Region meta is stale, return an error.
	if r.GetVersion() < o.GetVersion() || r.GetConfVer() < o.GetConfVer() {
		return errors.Trace(errRegionIsStale(region.Region, origin.Region))
	}
	// Region meta is updated, update kv and cache.
	if r.GetVersion() > o.GetVersion() {
		log.Infof("[region %d] %s, Version changed from {%d} to {%d}", region.GetId(), diffRegionKeyInfo(origin, region), o.GetVersion(), r.GetVersion())
		return c.putRegionLocked(region)
	}
	if r.GetConfVer() > o.GetConfVer() {
		log.Infof("[region %d] %s, ConfVer changed from {%d} to {%d}", region.GetId(), diffRegionPeersInfo(origin, region), o.GetConfVer(), r.GetConfVer())
		return c.putRegionLocked(region)
	}
	if region.Leader.GetId() != origin.Leader.GetId() {
		log.Infof("[region %d] Leader changed from {%v} to {%v}", region.GetId(), origin.GetPeer(origin.Leader.GetId()), region.GetPeer(region.Leader.GetId()))
	}
	// Region meta is the same, update cache only.
	c.regions.setRegion(region)
	return nil
}

func (c *clusterInfo) getRegionStores(region *regionInfo) []*storeInfo {
	c.RLock()
	defer c.RUnlock()
	var stores []*storeInfo
	for id := range region.GetStoreIds() {
		if store := c.stores.getStore(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *clusterInfo) allocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := c.allocID()
	if err != nil {
		return nil, errors.Trace(err)
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (c *clusterInfo) allocID() (uint64, error) {
	return c.id.Alloc()
}
