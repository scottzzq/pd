package server

import (
	"github.com/montanaflynn/stats"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"math"
	"time"
)

const (
	storeCacheInterval    = 30 * time.Second
	bootstrapBalanceCount = 10
	bootstrapBalanceDiff  = 2
)

// replicaChecker ensures region has the best replicas.
type replicaChecker struct {
	opt     *scheduleOption
	rep     *Replication
	cluster *clusterInfo
	//filters []Filter
}

func newReplicaChecker(opt *scheduleOption, cluster *clusterInfo) *replicaChecker {
	// var filters []Filter
	// filters = append(filters, newHealthFilter(opt))
	// filters = append(filters, newSnapshotCountFilter(opt))
	return &replicaChecker{
		opt:     opt,
		rep:     opt.GetReplication(),
		cluster: cluster,
		//filters: filters,
	}
}

func (r *replicaChecker) Check(region *regionInfo) Operator {
	//检查Down机的Peer所在的Store机器是否超过1小时，以及Leader上报的down机时间
	if op := r.checkDownPeer(region); op != nil {
		return op
	}
	//检查已经下线的Peer
	if op := r.checkOfflinePeer(region); op != nil {
		return op
	}

	log.Infof("replicaChecker Check, Region:[%v], peers_len:[%v], r.rep.GetMaxReplicas:[%v]",
		region, len(region.GetPeers()), r.rep.GetMaxReplicas())

	if len(region.GetPeers()) < r.rep.GetMaxReplicas() {
		log.Info("start selectBestPeer")
		newPeer, _ := r.selectBestPeer(region)
		if newPeer == nil {
			log.Infof("replicaChecker Check, Region:[%v], peers_len:[%v], r.rep.GetMaxReplicas:[%v] Select Peer null",
				region, len(region.GetPeers()), r.rep.GetMaxReplicas())
			return nil
		}
		log.Infof("find BestPeer:%v", newPeer)
		return newAddPeer(region, newPeer)
	}
	if len(region.GetPeers()) > r.rep.GetMaxReplicas() {
		oldPeer, _ := r.selectWorstPeer(region)
		if oldPeer == nil {
			return nil
		}
		return newRemovePeer(region, oldPeer)
	}
	return nil
	//return r.checkBestReplacement(region)
}

func (r *replicaChecker) checkDownPeer(region *regionInfo) Operator {
	for _, stats := range region.DownPeers {
		peer := stats.GetPeer()
		if peer == nil {
			continue
		}
		//根据down机的store-id
		store := r.cluster.getStore(peer.GetStoreId())
		//store down机时间是否超过1个小时
		if store.downTime() < r.opt.GetMaxStoreDownTime() {
			continue
		}
		//region的down机时间是否超过1小时
		if stats.GetDownSeconds() < uint64(r.opt.GetMaxStoreDownTime().Seconds()) {
			continue
		}
		return newRemovePeer(region, peer)
	}
	return nil
}

func (r *replicaChecker) checkOfflinePeer(region *regionInfo) Operator {
	//遍历当前Region的所有Peer,根据其store-id找到Store
	for _, peer := range region.GetPeers() {
		store := r.cluster.getStore(peer.GetStoreId())
		if store.isUp() {
			continue
		}
		//如果当前Store不在线，已经下线了，重新选一个Peer，为其添加副本
		newPeer, _ := r.selectBestPeer(region)
		if newPeer == nil {
			return nil
		}
		return newTransferPeer(region, peer, newPeer)
	}
	return nil
}

func (r *replicaChecker) checkBestReplacement(region *regionInfo) Operator {
	oldPeer, oldScore := r.selectWorstPeer(region)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.selectBestReplacement(region, oldPeer)
	if newPeer == nil {
		return nil
	}
	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}
	return newTransferPeer(region, oldPeer, newPeer)
}

// selectBestPeer returns the best peer in other stores.
func (r *replicaChecker) selectBestPeer(region *regionInfo /*, filters ...Filter*/) (*metapb.Peer, float64) {
	var filters []Filter
	filters = append(filters, newExcludedFilter(nil, region.GetStoreIds()))

	var (
		bestStore *storeInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range r.cluster.getStores() {
		if filterTarget(store, filters) {
			continue
		}
		score := r.rep.GetDistinctScore(stores, store)
		if bestStore == nil || compareStoreScore(store, score, bestStore, bestScore) > 0 {
			bestStore = store
			bestScore = score
		}
	}
	if bestStore == nil { //|| filterTarget(bestStore, r.filters) {
		return nil, 0
	}
	newPeer, err := r.cluster.allocPeer(bestStore.GetId())
	if err != nil {
		log.Errorf("failed to allocate peer: %v", err)
		return nil, 0
	}
	return newPeer, bestScore
}

// selectWorstPeer returns the worst peer in the region.
func (r *replicaChecker) selectWorstPeer(region *regionInfo) (*metapb.Peer, float64) {
	var (
		worstStore *storeInfo
		worstScore float64
	)
	// Select the store with lowest distinct score.
	// If the scores are the same, select the store with maximal region score.
	stores := r.cluster.getRegionStores(region)
	for _, store := range stores {
		score := r.rep.GetDistinctScore(stores, store)
		if worstStore == nil || compareStoreScore(store, score, worstStore, worstScore) < 0 {
			worstStore = store
			worstScore = score
		}
	}
	if worstStore == nil {
		return nil, 0
	}
	return region.GetStorePeer(worstStore.GetId()), worstScore
}

// selectBestReplacement returns the best peer to replace the region peer.
func (r *replicaChecker) selectBestReplacement(region *regionInfo, peer *metapb.Peer) (*metapb.Peer, float64) {
	// Get a new region without the peer we are going to replace.
	newRegion := region.clone()
	newRegion.RemoveStorePeer(peer.GetStoreId())
	return r.selectBestPeer(newRegion)
}

//对各个store上的leader数量做调度
type balanceLeaderScheduler struct {
	opt      *scheduleOption
	limit    uint64
	selector Selector
}

func newBalanceLeaderScheduler(opt *scheduleOption) *balanceLeaderScheduler {
	var filters []Filter
	// filters = append(filters, newBlockFilter())
	// filters = append(filters, newStateFilter(opt))
	// filters = append(filters, newHealthFilter(opt))
	return &balanceLeaderScheduler{
		opt:      opt,
		limit:    1,
		selector: newBalanceSelector(leaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return "balance-leader-scheduler"
}

func (l *balanceLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.opt.GetLeaderScheduleLimit())
}

func (l *balanceLeaderScheduler) Prepare(cluster *clusterInfo) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cluster *clusterInfo) {}

func (l *balanceLeaderScheduler) Schedule(cluster *clusterInfo) Operator {
	//选中Reion，以及新Leader
	region, newLeader := scheduleTransferLeader(cluster, l.selector)
	//log.Infof("balanceLeaderScheduler Schedule, region:[%v] newLeader:[%v]", region, newLeader)
	if region == nil {
		return nil
	}
	source := cluster.getStore(region.Leader.GetStoreId())
	target := cluster.getStore(newLeader.GetStoreId())
	if !shouldBalance(source, target, l.GetResourceKind()) {
		return nil
	}
	l.limit = adjustBalanceLimit(cluster, l.GetResourceKind())
	return newTransferLeader(region, newLeader)
}

// shouldBalance returns true if we should balance the source and target store.
// The min balance diff provides a buffer to make the cluster stable, so that we
// don't need to schedule very frequently.
func shouldBalance(source, target *storeInfo, kind ResourceKind) bool {
	sourceCount := source.resourceCount(kind)
	sourceScore := source.resourceScore(kind)
	targetScore := target.resourceScore(kind)
	if targetScore >= sourceScore {
		return false
	}
	diffRatio := 1 - targetScore/sourceScore
	diffCount := diffRatio * float64(sourceCount)
	return diffCount >= minBalanceDiff(sourceCount)
}

func adjustBalanceLimit(cluster *clusterInfo, kind ResourceKind) uint64 {
	stores := cluster.getStores()
	counts := make([]float64, 0, len(stores))
	for _, s := range stores {
		counts = append(counts, float64(s.resourceCount(kind)))
	}
	limit, _ := stats.StandardDeviation(stats.Float64Data(counts))
	return maxUint64(1, uint64(limit))
}

// minBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func minBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}
