package server

import (
	_ "bytes"

	"github.com/gogo/protobuf/proto"
	_ "github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	defaultBTreeDegree = 64
)

// TODO: Export this to API directly.
type regionInfo struct {
	*metapb.Region
	Leader *metapb.Peer
	// 	message PeerStats {
	//     optional metapb.Peer peer    = 1;
	//     optional uint64 down_seconds = 2;
	// }
	DownPeers    []*pdpb.PeerStats
	PendingPeers []*metapb.Peer
}

func newRegionInfo(region *metapb.Region, leader *metapb.Peer) *regionInfo {
	return &regionInfo{
		Region: region,
		Leader: leader,
	}
}

func (r *regionInfo) clone() *regionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.DownPeers))
	for _, peer := range r.DownPeers {
		downPeers = append(downPeers, proto.Clone(peer).(*pdpb.PeerStats))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.PendingPeers))
	for _, peer := range r.PendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}
	return &regionInfo{
		Region:       proto.Clone(r.Region).(*metapb.Region),
		Leader:       proto.Clone(r.Leader).(*metapb.Peer),
		DownPeers:    downPeers,
		PendingPeers: pendingPeers,
	}
}

func (r *regionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

func (r *regionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.PendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

func (r *regionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.DownPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

func (r *regionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

func (r *regionInfo) RemoveStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range r.GetPeers() {
		if peer.GetStoreId() != storeID {
			peers = append(peers, peer)
		}
	}
	r.Peers = peers
}

func (r *regionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

func (r *regionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetPeers()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.Leader == nil || r.Leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}
