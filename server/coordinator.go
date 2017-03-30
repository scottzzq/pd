package server

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

const (
	historiesCacheSize  = 1000
	eventsCacheSize     = 1000
	maxScheduleRetries  = 10
	maxScheduleInterval = time.Minute
	minScheduleInterval = time.Millisecond * 10
)

var (
	errSchedulerExisted  = errors.New("scheduler is existed")
	errSchedulerNotFound = errors.New("scheduler is not found")
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster *clusterInfo
	opt     *scheduleOption
	//limiter    *scheduleLimiter
	checker   *replicaChecker
	operators map[uint64]Operator
	//schedulers map[string]*scheduleController

	//histories *lruCache
	//events    *fifoCache
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:     ctx,
		cancel:  cancel,
		cluster: cluster,
		opt:     opt,
		//limiter:    newScheduleLimiter(),
		checker:   newReplicaChecker(opt, cluster),
		operators: make(map[uint64]Operator),
		//schedulers: make(map[string]*scheduleController),
		//histories:  newLRUCache(historiesCacheSize),
		//events:     newFifoCache(eventsCacheSize),
	}
}

func (c *coordinator) dispatch(region *regionInfo) *pdpb.RegionHeartbeatResponse {
	log.Info("coordinator dispatch")
	// Check existed operator.
	if op := c.getOperator(region.GetId()); op != nil {
		res, finished := op.Do(region)
		if !finished {
			return res
		}
		c.removeOperator(op)
	}

	// // Check replica operator.
	// if c.limiter.operatorCount(regionKind) >= c.opt.GetReplicaScheduleLimit() {
	// 	return nil
	// }
	//调用副本检查器，检查副本数量
	if op := c.checker.Check(region); op != nil {
		if c.addOperator(op) {
			res, _ := op.Do(region)
			return res
		}
	}
	return nil
}

func (c *coordinator) addOperator(op Operator) bool {
	c.Lock()
	defer c.Unlock()
	regionID := op.GetRegionID()
	// Admin operator bypasses the check.
	if op.GetResourceKind() == adminKind {
		c.operators[regionID] = op
		return true
	}
	if _, ok := c.operators[regionID]; ok {
		return false
	}
	//c.limiter.addOperator(op)
	c.operators[regionID] = op
	//collectOperatorCounterMetrics(op)
	return true
}

func (c *coordinator) getOperator(regionID uint64) Operator {
	c.RLock()
	defer c.RUnlock()
	return c.operators[regionID]
}

func (c *coordinator) removeOperator(op Operator) {
	c.Lock()
	defer c.Unlock()

	regionID := op.GetRegionID()
	//c.limiter.removeOperator(op)
	delete(c.operators, regionID)

	//c.histories.add(regionID, op)
}

func (c *coordinator) run() {
	//c.addScheduler(newBalanceLeaderScheduler(c.opt))
	//c.addScheduler(newBalanceRegionScheduler(c.opt))
}
