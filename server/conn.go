// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bufio"
	"io"
	"net"
	"strings"
	_ "time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
)

const (
	readBufferSize  = 8 * 1024
	writeBufferSize = 8 * 1024
)

//接收的连接句柄
type conn struct {
	s *Server

	rb   *bufio.Reader
	wb   *bufio.Writer
	conn net.Conn
}

func newConn(s *Server, netConn net.Conn, bufrw *bufio.ReadWriter) (*conn, error) {
	s.connsLock.Lock()
	defer s.connsLock.Unlock()
	c := &conn{
		s:    s,
		rb:   bufrw.Reader,
		wb:   bufrw.Writer,
		conn: netConn,
	}
	s.conns[c] = struct{}{}
	return c, nil
}

func (c *conn) run() {
	defer func() {
		c.s.wg.Done()
		c.close()

		c.s.connsLock.Lock()
		delete(c.s.conns, c)
		c.s.connsLock.Unlock()
	}()
	//leader代理器
	p := &leaderProxy{s: c.s}
	defer p.close()
	for {
		msg := &msgpb.Message{}
		//解析消息
		msgID, err := util.ReadMessage(c.rb, msg)
		if err != nil {
			if isUnexpectedConnError(err) {
				log.Errorf("read request message err %v", err)
			}
			return
		}
		//判断消息类型
		if msg.GetMsgType() != msgpb.MessageType_PdReq {
			log.Errorf("invalid request message %v", msg)
			return
		}
		//start := time.Now()
		request := msg.GetPdReq()
		var response *pdpb.Response

		//校验cluster id
		//dev
		// if err = c.checkRequest(request); err != nil {
		// 	log.Errorf("check request %s err %v", request, errors.ErrorStack(err))
		// 	response = newError(err)
		// } else
		if !c.s.IsLeader() {
			//不是leader，需要调用leader proxy处理，先不考虑
			// response, err = p.handleRequest(msgID, request)
			// if err != nil {
			// 	if isUnexpectedConnError(err) {
			// 		log.Errorf("proxy request %s err %v", request, errors.ErrorStack(err))
			// 	}
			// 	response = newError(err)
			// }
		} else {
			//当前就是leader，处理消息
			response, err = c.handleRequest(request)
			if err != nil {
				if isUnexpectedConnError(err) {
					log.Errorf("handle request %s err %v", request, errors.ErrorStack(err))
				}
				response = newError(err)
			}
		}
		if response == nil {
			// we don't need to response, maybe error?
			// if error, we will return an error response later.
			log.Warn("empty response")
			continue
		}
		updateResponse(request, response)
		msg = &msgpb.Message{
			MsgType: msgpb.MessageType_PdResp,
			PdResp:  response,
		}
		if err = util.WriteMessage(c.wb, msgID, msg); err != nil {
			if isUnexpectedConnError(err) {
				log.Errorf("write response message err %v", err)
			}
			return
		}

		if err = c.wb.Flush(); err != nil {
			if isUnexpectedConnError(err) {
				log.Errorf("flush response message err %v", err)
			}
			return
		}
	}
}

//校验消息，校验cluster id
func (c *conn) checkRequest(req *pdpb.Request) error {
	// Don't check cluster ID of this command type.
	if req.GetCmdType() == pdpb.CommandType_GetPDMembers {
		if req.Header == nil {
			req.Header = &pdpb.RequestHeader{}
		}
		req.Header.ClusterId = c.s.clusterID
	}

	clusterID := req.GetHeader().GetClusterId()
	if clusterID != c.s.clusterID {
		return errors.Errorf("mismatch cluster id, need %d but got %d", c.s.clusterID, clusterID)
	}
	return nil
}

func (c *conn) close() error {
	return nil
}

//leader代理器
type leaderProxy struct {
	s    *Server
	conn net.Conn
}

func (p *leaderProxy) close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

var errClosed = errors.New("use of closed network connection")

func isUnexpectedConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Cause(err) == io.EOF {
		return false
	}
	if strings.Contains(err.Error(), errClosed.Error()) {
		return false
	}
	return true
}

func updateResponse(req *pdpb.Request, resp *pdpb.Response) {
	// We can use request field directly here.
	resp.CmdType = req.CmdType
	if req.Header == nil {
		return
	}
	if resp.Header == nil {
		resp.Header = &pdpb.ResponseHeader{}
	}
	resp.Header.Uuid = req.Header.Uuid
	resp.Header.ClusterId = req.Header.ClusterId
}

//enum CommandType {
//    Invalid             = 0;
//    Tso                 = 1;
//    Bootstrap           = 2;
//    IsBootstrapped      = 3;
//    AllocId             = 4;
//    GetStore            = 5;
//    PutStore            = 6;
//    AskSplit            = 7;
//    GetRegion           = 8;
//    RegionHeartbeat     = 9;
//    GetClusterConfig    = 10;
//    PutClusterConfig    = 11;
//    StoreHeartbeat      = 12;
//    ReportSplit         = 13;
//    GetRegionByID       = 14;
//    GetPDMembers        = 15;
//}
func (c *conn) handleRequest(req *pdpb.Request) (*pdpb.Response, error) {
	switch req.GetCmdType() {
	//判断集群是否初始化
	case pdpb.CommandType_IsBootstrapped:
		log.Infof("Receive Cmd: IsBootstrapped: %v", req)
		return c.handleIsBootstrapped(req)
	// 初始化集群
	case pdpb.CommandType_Bootstrap:
		log.Infof("Receive Cmd: Bootstrap: %v", req)
		return c.handleBootstrap(req)
	// store启动的时候上报
	case pdpb.CommandType_PutStore:
		log.Infof("Receive Cmd: PutStore: %v", req)
		return c.handlePutStore(req)
	// store心跳
	case pdpb.CommandType_StoreHeartbeat:
		log.Infof("Receive Cmd: StoreHeartbeat: %v", req)
		return c.handleStoreHeartbeat(req)
	//region心跳
	case pdpb.CommandType_RegionHeartbeat:
		log.Infof("Receive Cmd: RegionHeartbeat: %v", req)
		return c.handleRegionHeartbeat(req)
	//分配id
	case pdpb.CommandType_AllocId:
		log.Infof("Receive Cmd: AllocId: %v", req)
		return c.handleAllocID(req)
	//获取store，解析地址
	case pdpb.CommandType_GetStore:
		log.Infof("Receive Cmd: GetStore: %v", req)
		return c.handleGetStore(req)
	// 获取Region
	case pdpb.CommandType_GetRegionByID:
		return c.handleGetRegionByID(req)

	// 获取region
	// case pdpb.CommandType_GetRegion:
	// 	return c.handleGetRegion(req)
	// //获取region
	// case pdpb.CommandType_Tso:
	// 	return c.handleTso(req)
	// case pdpb.CommandType_AllocId:
	// 	return c.handleAllocID(req)
	// case pdpb.CommandType_AskSplit:
	// 	return c.handleAskSplit(req)
	// case pdpb.CommandType_ReportSplit:
	// 	return c.handleReportSplit(req)
	// case pdpb.CommandType_GetClusterConfig:
	// 	return c.handleGetClusterConfig(req)
	// case pdpb.CommandType_PutClusterConfig:
	// 	return c.handlePutClusterConfig(req)
	// case pdpb.CommandType_GetPDMembers:
	// 	return c.handleGetPDMembers(req)
	default:
		log.Infof("unsupported command: %v", req)
		return nil, errors.Errorf("unsupported command %s", req)
	}
}
