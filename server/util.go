package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	_ "math/rand"
	_ "net"
	_ "net/http"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/pkg/capnslog"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	_ "github.com/pingcap/kvproto/pkg/msgpb"
	_ "github.com/pingcap/kvproto/pkg/pdpb"
	_ "github.com/pingcap/kvproto/pkg/util"
	"golang.org/x/net/context"
	"pd/pkg/etcdutil"
)

const (
	requestTimeout  = etcdutil.DefaultRequestTimeout
	slowRequestTime = etcdutil.DefaultSlowRequestTime

	logDirMode = 0755
)

// Version information.
var (
	PDBuildTS = "None"
	PDGitHash = "None"
)

// LogPDInfo prints the PD version information.
func LogPDInfo() {
	log.Infof("Welcome to Placement Driver (PD).")
	log.Infof("Version:")
	log.Infof("Git Commit Hash: %s", PDGitHash)
	log.Infof("UTC Build Time:  %s", PDBuildTS)
}

// PrintPDInfo prints the PD version information without log info.
func PrintPDInfo() {
	fmt.Println("Git Commit Hash:", PDGitHash)
	fmt.Println("UTC Build Time: ", PDBuildTS)
}

// A helper function to get value with key from etcd.
// TODO: return the value revision for outer use.
func getValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := kvGet(c, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

// Return boolean to indicate whether the key exists or not.
// TODO: return the value revision for outer use.
func getProtoMsg(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, error) {
	value, err := getValue(c, key, opts...)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}

	if err = proto.Unmarshal(value, msg); err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
}

func initOrGetClusterID(c *clientv3.Client, key string, clusterID uint64) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	//ts := uint64(time.Now().Unix())
	//clusterID := (ts << 32) + uint64(rand.Uint32())
	value := uint64ToBytes(clusterID)

	// Multiple PDs may try to init the cluster ID at the same time.
	// Only one PD can commit this transaction, then other PDs can get
	// the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.Errorf("txn returns empty response: %v", resp)
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.Errorf("txn returns invalid range response: %v", resp)
	}

	return bytesToUint64(response.Kvs[0].Value)
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

type redirectFormatter struct{}

// Format turns capnslog logs to ngaut logs.
// TODO: remove ngaut log caller stack, "util.go:xxx"
func (rf *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	if pkg != "" {
		pkg = fmt.Sprint(pkg, ": ")
	}

	logStr := fmt.Sprint(level.Char(), " | ", pkg, entries)

	switch level {
	case capnslog.CRITICAL:
		log.Fatalf(logStr)
	case capnslog.ERROR:
		log.Errorf(logStr)
	case capnslog.WARNING:
		log.Warningf(logStr)
	case capnslog.NOTICE:
		log.Infof(logStr)
	case capnslog.INFO:
		log.Infof(logStr)
	case capnslog.DEBUG:
		log.Debugf(logStr)
	case capnslog.TRACE:
		log.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (rf *redirectFormatter) Flush() {}

// setLogOutput sets output path for all logs.
func setLogOutput(logFile string) error {
	// PD log.
	dir := path.Dir(logFile)
	err := os.MkdirAll(dir, logDirMode)
	if err != nil {
		return errors.Trace(err)
	}
	log.SetOutputByName(logFile)
	log.SetRotateByDay()

	// ETCD log.
	capnslog.SetFormatter(&redirectFormatter{})

	return nil
}

// InitLogger initalizes PD's logger.
func InitLogger(cfg *Config) error {
	log.SetLevelByString(cfg.LogLevel)
	log.SetHighlighting(false)

	// Force redirect etcd log to stderr.
	if len(cfg.LogFile) == 0 {
		capnslog.SetFormatter(capnslog.NewPrettyFormatter(os.Stderr, false))
		return nil
	}

	err := setLogOutput(cfg.LogFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func diffRegionPeersInfo(origin *regionInfo, other *regionInfo) string {
	var ret []string
	for _, a := range origin.Peers {
		both := false
		for _, b := range other.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.Peers {
		both := false
		for _, a := range origin.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

func diffRegionKeyInfo(origin *regionInfo, other *regionInfo) string {
	var ret []string
	if !bytes.Equal(origin.Region.StartKey, other.Region.StartKey) {
		originKey := &metapb.Region{StartKey: origin.Region.StartKey}
		otherKey := &metapb.Region{StartKey: other.Region.StartKey}
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", originKey, otherKey))
	}
	if !bytes.Equal(origin.Region.EndKey, other.Region.EndKey) {
		originKey := &metapb.Region{EndKey: origin.Region.EndKey}
		otherKey := &metapb.Region{EndKey: other.Region.EndKey}
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", originKey, otherKey))
	}

	return strings.Join(ret, ",")
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
