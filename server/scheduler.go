package server

import (
	_ "fmt"

	_ "github.com/juju/errors"
	_ "github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() ResourceKind
	GetResourceLimit() uint64
	Prepare(cluster *clusterInfo) error
	Cleanup(cluster *clusterInfo)
	Schedule(cluster *clusterInfo) Operator
}

func newAddPeer(region *regionInfo, peer *metapb.Peer) Operator {
	addPeer := newAddPeerOperator(region.GetId(), peer)
	return newRegionOperator(region, addPeer)
}

func newRemovePeer(region *regionInfo, peer *metapb.Peer) Operator {
	removePeer := newRemovePeerOperator(region.GetId(), peer)
	return newRegionOperator(region, removePeer)
}

func newTransferPeer(region *regionInfo, oldPeer, newPeer *metapb.Peer) Operator {
	addPeer := newAddPeerOperator(region.GetId(), newPeer)
	removePeer := newRemovePeerOperator(region.GetId(), oldPeer)
	return newRegionOperator(region, addPeer, removePeer)
}

func newTransferLeader(region *regionInfo, newLeader *metapb.Peer) Operator {
	transferLeader := newTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return newRegionOperator(region, transferLeader)
}

// scheduleTransferLeader schedules a region to transfer leader to the peer.
func scheduleTransferLeader(cluster *clusterInfo, s Selector, filters ...Filter) (*regionInfo, *metapb.Peer) {
	sourceStores := cluster.getStores()

	source := s.SelectSource(sourceStores, filters...)
	if source == nil {
		return nil, nil
	}

	region := cluster.randLeaderRegion(source.GetId())
	if region == nil {
		return nil, nil
	}

	targetStores := cluster.getFollowerStores(region)

	target := s.SelectTarget(targetStores)
	if target == nil {
		return nil, nil
	}

	return region, region.GetStorePeer(target.GetId())
}

// scheduleRemovePeer schedules a region to remove the peer.
func scheduleRemovePeer(cluster *clusterInfo, s Selector, filters ...Filter) (*regionInfo, *metapb.Peer) {
	stores := cluster.getStores()

	source := s.SelectSource(stores, filters...)
	if source == nil {
		return nil, nil
	}

	region := cluster.randFollowerRegion(source.GetId())
	if region == nil {
		region = cluster.randLeaderRegion(source.GetId())
	}
	if region == nil {
		return nil, nil
	}

	return region, region.GetStorePeer(source.GetId())
}
