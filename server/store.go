package server

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	adminKind ResourceKind = iota
	leaderKind
	regionKind
)

// storeInfo contains information about a store.
// TODO: Export this to API directly.
type storeInfo struct {
	*metapb.Store
	status *StoreStatus
}

func newStoreInfo(store *metapb.Store) *storeInfo {
	return &storeInfo{
		Store:  store,
		status: newStoreStatus(),
	}
}

func (s *storeInfo) clone() *storeInfo {
	return &storeInfo{
		Store:  proto.Clone(s.Store).(*metapb.Store),
		status: s.status.clone(),
	}
}

func (s *storeInfo) isUp() bool {
	return s.GetState() == metapb.StoreState_Up
}

func (s *storeInfo) isOffline() bool {
	return s.GetState() == metapb.StoreState_Offline
}

func (s *storeInfo) isTombstone() bool {
	return s.GetState() == metapb.StoreState_Tombstone
}

func (s *storeInfo) getLabelValue(key string) string {
	for _, label := range s.GetLabels() {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}

func (s *storeInfo) downTime() time.Duration {
	return time.Since(s.status.LastHeartbeatTS)
}

func (s *storeInfo) getLocationID(keys []string) string {
	id := ""
	for _, k := range keys {
		v := s.getLabelValue(k)
		if len(v) == 0 {
			return ""
		}
		id += v
	}
	return id
}

func (s *storeInfo) resourceCount(kind ResourceKind) uint64 {
	switch kind {
	case leaderKind:
		return s.leaderCount()
	case regionKind:
		return s.regionCount()
	default:
		return 0
	}
}

func (s *storeInfo) resourceScore(kind ResourceKind) float64 {
	switch kind {
	case leaderKind:
		return s.leaderScore()
	case regionKind:
		return s.regionScore()
	default:
		return 0
	}
}

func (s *storeInfo) leaderCount() uint64 {
	return uint64(s.status.LeaderCount)
}

func (s *storeInfo) leaderScore() float64 {
	return float64(s.status.LeaderCount)
}

func (s *storeInfo) regionCount() uint64 {
	return uint64(s.status.RegionCount)
}

func (s *storeInfo) regionScore() float64 {
	if s.status.GetCapacity() == 0 {
		return 0
	}
	return float64(s.status.RegionCount) / float64(s.status.GetCapacity())
}

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	*pdpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked         bool
	LeaderCount     uint32    `json:"leader_count"`
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
}

func newStoreStatus() *StoreStatus {
	return &StoreStatus{
		StoreStats: &pdpb.StoreStats{},
	}
}
func (s *StoreStatus) clone() *StoreStatus {
	return &StoreStatus{
		StoreStats:      proto.Clone(s.StoreStats).(*pdpb.StoreStats),
		blocked:         s.blocked,
		LeaderCount:     s.LeaderCount,
		LastHeartbeatTS: s.LastHeartbeatTS,
	}
}
