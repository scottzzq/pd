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
	maxScheduleRetries  = 2
	maxScheduleInterval = time.Minute * 2
	minScheduleInterval = time.Millisecond * 15000
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

	cluster    *clusterInfo
	opt        *scheduleOption
	limiter    *scheduleLimiter
	checker    *replicaChecker
	operators  map[uint64]Operator
	schedulers map[string]*scheduleController
	//histories *lruCache
	//events    *fifoCache
}

func newCoordinator(cluster *clusterInfo, opt *scheduleOption) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:        ctx,
		cancel:     cancel,
		cluster:    cluster,
		opt:        opt,
		limiter:    newScheduleLimiter(),
		checker:    newReplicaChecker(opt, cluster),
		operators:  make(map[uint64]Operator),
		schedulers: make(map[string]*scheduleController),
		//histories:  newLRUCache(historiesCacheSize),
		//events:     newFifoCache(eventsCacheSize),
	}
}

func (c *coordinator) dispatch(region *regionInfo) *pdpb.RegionHeartbeatResponse {
	log.Info("coordinator dispatch")
	// Check existed operator.
	if op := c.getOperator(region.GetId()); op != nil {
		res, finished := op.Do(region)
		log.Infof("region:%v finished:%v op:%v", region, finished, op)
		if !finished {
			return res
		}
		c.removeOperator(op)
	}

	// // Check replica operator.
	if c.limiter.operatorCount(regionKind) >= c.opt.GetReplicaScheduleLimit() {
		return nil
	}
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
	c.limiter.addOperator(op)
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
	c.limiter.removeOperator(op)
	delete(c.operators, regionID)
	//c.histories.add(regionID, op)
}

type scheduleLimiter struct {
	sync.RWMutex
	counts map[ResourceKind]uint64
}

func newScheduleLimiter() *scheduleLimiter {
	return &scheduleLimiter{
		counts: make(map[ResourceKind]uint64),
	}
}

func (l *scheduleLimiter) addOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]++
}

func (l *scheduleLimiter) removeOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]--
}

func (l *scheduleLimiter) operatorCount(kind ResourceKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.counts[kind]
}

type scheduleController struct {
	Scheduler
	opt      *scheduleOption
	limiter  *scheduleLimiter
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
}

func newScheduleController(c *coordinator, s Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler: s,
		opt:       c.opt,
		limiter:   c.limiter,
		interval:  minScheduleInterval,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *scheduleController) GetInterval() time.Duration {
	return s.interval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.limiter.operatorCount(s.GetResourceKind()) < s.GetResourceLimit()
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (c *coordinator) addScheduler(scheduler Scheduler) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	return nil
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			for i := 0; i < maxScheduleRetries; i++ {
				op := s.Schedule(c.cluster)
				if op == nil {
					continue
				}
				log.Infof("===========runScheduler, Op:[%v]", op)
				if c.addOperator(op) {
					break
				}
			}
		case <-s.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

func (c *coordinator) run() {
	c.addScheduler(newBalanceLeaderScheduler(c.opt))
	//c.addScheduler(newBalanceRegionScheduler(c.opt))
}
