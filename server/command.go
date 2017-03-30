package server

import (
	_ "time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var (
	errNotBootstrapped = errors.New("TiKV cluster is not bootstrapped, please start TiKV first")
)

func (c *conn) getRaftCluster() (*RaftCluster, error) {
	cluster := c.s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.Trace(errNotBootstrapped)
	}
	return cluster, nil
}

//////////////////////处理解析Store地址、、、、、、、、、、、、、、、
// message GetStoreRequest {
//     optional uint64 store_id       = 1 [(gogoproto.nullable) = false];
// }
// message GetStoreResponse {
//     optional metapb.Store store     = 1;
// }
func (c *conn) handleGetStore(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetStore()
	if request == nil {
		return nil, errors.Errorf("invalid get store command, but %v", req)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	storeID := request.GetStoreId()
	store, _, err := cluster.GetStore(storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb.Response{
		GetStore: &pdpb.GetStoreResponse{
			Store: store,
		},
	}, nil
}

///////////////////////初始化集群、、、、、、、、、、、、
// message BootstrapRequest {
//     optional metapb.Store store   = 1;
//     optional metapb.Region region = 2;
// }
// message BootstrapResponse {
// }
func (c *conn) handleBootstrap(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetBootstrap()
	if request == nil {
		return nil, errors.Errorf("invalid bootstrap command, but %v", req)
	}
	cluster := c.s.GetRaftCluster()
	if cluster != nil {
		return newBootstrappedError(), nil
	}
	//dev
	return c.s.BootstrapCluster(request)
}

//////////////////////Store启动上报、、、、、、、、、、、、、
// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func checkStore(cluster *RaftCluster, storeID uint64) *pdpb.Response {
	store, _, err := cluster.GetStore(storeID)
	if err == nil && store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return newStoreIsTombstoneError()
		}
	}
	return nil
}

// message PutStoreRequest {
//     optional metapb.Store store     = 1;
// }
// message BootstrapResponse {
// }
func (c *conn) handlePutStore(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetPutStore()
	if request == nil {
		return nil, errors.Errorf("invalid put store command, but %v", req)
	}
	store := request.GetStore()
	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp := checkStore(cluster, store.GetId()); resp != nil {
		return resp, nil
	}
	if err = cluster.putStore(store); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("put store ok - %v", store)
	return &pdpb.Response{
		PutStore: &pdpb.PutStoreResponse{},
	}, nil
}

//////////////////////Store心跳、、、、、、、、、、、、、
// message RegionHeartbeatRequest {
//     optional metapb.Region region       = 1;
//     // Leader Peer sending the heartbeat.
//     optional metapb.Peer leader         = 2;
//     // Leader considers that these peers are down.
//     repeated PeerStats down_peers       = 3;
//     // Pending peers are the peers that the leader can't consider as
//     // working followers.
//     repeated metapb.Peer pending_peers  = 4;
//     // Bytes read/written during this period.
//     optional uint64 bytes_written       = 5;
//     optional uint64 bytes_read          = 6;
//     // Keys read/written during this period.
//     optional uint64 keys_written        = 7;
//     optional uint64 keys_read           = 8;
// }
// message StoreStats {
//     optional uint64 store_id             = 1 [(gogoproto.nullable) = false];
//     // Capacity for the store.
//     optional uint64 capacity             = 2 [(gogoproto.nullable) = false];
//     // Available size for the store.
//     optional uint64 available            = 3 [(gogoproto.nullable) = false];
//     // Total region count in this store.
//     optional uint32 region_count         = 4 [(gogoproto.nullable) = false];
//     // Current sending snapshot count.
//     optional uint32 sending_snap_count   = 5 [(gogoproto.nullable) = false];
//     // Current receiving snapshot count.
//     optional uint32 receiving_snap_count = 6 [(gogoproto.nullable) = false];
//     // When the store is started (unix timestamp in seconds).
//     optional uint32 start_time           = 7 [(gogoproto.nullable) = false];
//     // How many region is applying snapshot.
//     optional uint32 applying_snap_count  = 8 [(gogoproto.nullable) = false];
//     // If the store is busy
//     optional bool is_busy                = 9 [(gogoproto.nullable) = false];
// }
// message StoreHeartbeatRequest {
//     optional StoreStats stats    = 1;
// }
// message StoreHeartbeatResponse {
//}
func (c *conn) handleStoreHeartbeat(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetStoreHeartbeat()
	stats := request.GetStats()
	if stats == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}

	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp := checkStore(cluster, stats.GetStoreId()); resp != nil {
		return resp, nil
	}

	err = cluster.cachedCluster.handleStoreHeartbeat(stats)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.Response{
		StoreHeartbeat: &pdpb.StoreHeartbeatResponse{},
	}, nil
}

//////////////////////Region心跳、、、、、、、、、、、、、
// message ChangePeer {
//     optional eraftpb.ConfChangeType change_type = 1;
//     optional metapb.Peer peer                   = 2;
// }

// message TransferLeader {
//     optional metapb.Peer peer = 1;
// }

// message RegionHeartbeatResponse {
//     // Notice, Pd only allows handling reported epoch >= current pd's.
//     // Leader peer reports region status with RegionHeartbeatRequest
//     // to pd regularly, pd will determine whether this region
//     // should do ChangePeer or not.
//     // E,g, max peer number is 3, region A, first only peer 1 in A.
//     // 1. Pd region state -> Peers (1), ConfVer (1).
//     // 2. Leader peer 1 reports region state to pd, pd finds the
//     // peer number is < 3, so first changes its current region
//     // state -> Peers (1, 2), ConfVer (1), and returns ChangePeer Adding 2.
//     // 3. Leader does ChangePeer, then reports Peers (1, 2), ConfVer (2),
//     // pd updates its state -> Peers (1, 2), ConfVer (2).
//     // 4. Leader may report old Peers (1), ConfVer (1) to pd before ConfChange
//     // finished, pd stills responses ChangePeer Adding 2, of course, we must
//     // guarantee the second ChangePeer can't be applied in TiKV.
//     optional ChangePeer change_peer           = 1;
//     // Pd can return transfer_leader to let TiKV does leader transfer itself.
//     optional TransferLeader transfer_leader   = 2;
// }
func (c *conn) handleRegionHeartbeat(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetRegionHeartbeat()
	if request == nil {
		return nil, errors.Errorf("invalid region heartbeat command, but %v", request)
	}
	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	region := newRegionInfo(request.GetRegion(), request.GetLeader())
	region.DownPeers = request.GetDownPeers()
	region.PendingPeers = request.GetPendingPeers()
	if region.GetId() == 0 {
		return nil, errors.Errorf("invalid request region, %v", request)
	}
	if region.Leader == nil {
		return nil, errors.Errorf("invalid request leader, %v", request)
	}
	// 处理Region映射关系
	// clusterInfo cache.go
	err = cluster.cachedCluster.handleRegionHeartbeat(region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//根据当前Region的详细信息，执行调度
	// RaftCluster cluster.go
	res, err := cluster.handleRegionHeartbeat(region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdpb.Response{
		RegionHeartbeat: res,
	}, nil
}

///////////////////分配唯一id、、、、、、、、、、、、、、、、、、、、
// message AllocIdRequest {
// }
// message AllocIdResponse {
//     optional uint64 id             = 1 [(gogoproto.nullable) = false];
// }
func (c *conn) handleAllocID(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetAllocId()
	if request == nil {
		return nil, errors.Errorf("invalid alloc id command, but %v", req)
	}
	// We can use an allocator for all types ID allocation.
	id, err := c.s.idAlloc.Alloc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	idResp := &pdpb.AllocIdResponse{
		Id: id,
	}
	return &pdpb.Response{
		AllocId: idResp,
	}, nil
}

////////////////////////////根据Region ID获取 Region
func (c *conn) handleGetRegionByID(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetGetRegionById()
	if request == nil {
		return nil, errors.Errorf("invalid get region by id command, but %v", req)
	}
	cluster, err := c.getRaftCluster()
	if err != nil {
		return nil, errors.Trace(err)
	}
	id := request.GetRegionId()
	region, leader := cluster.GetRegionByID(id)
	return &pdpb.Response{
		GetRegionById: &pdpb.GetRegionResponse{
			Region: region,
			Leader: leader,
		},
	}, nil
}

func (c *conn) handleIsBootstrapped(req *pdpb.Request) (*pdpb.Response, error) {
	request := req.GetIsBootstrapped()
	if request == nil {
		return nil, errors.Errorf("invalid is bootstrapped command, but %v", req)
	}
	cluster := c.s.GetRaftCluster()
	resp := &pdpb.IsBootstrappedResponse{
		Bootstrapped: proto.Bool(cluster != nil),
	}
	return &pdpb.Response{
		IsBootstrapped: resp,
	}, nil
}
