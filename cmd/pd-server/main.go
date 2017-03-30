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

package main

import (
	"flag"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"os"
	"os/signal"
	"syscall"
	"time"
	// "pd/pkg/metricutil"
	"pd/server"
	//"pd/server/api"
)

func main() {
	cfg := server.NewConfig()
	err := cfg.Parse(os.Args[1:])

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags error %s\n", err)
	}

	err = server.InitLogger(cfg)
	if err != nil {
		log.Fatalf("initalize logger error %s\n", err)
	}

	server.LogPDInfo()

	//	err = server.PrepareJoinCluster(cfg)
	//	if err != nil {
	//		log.Fatal("join error ", err)
	//	}
	svr := server.CreateServer(cfg)
	err = svr.StartEtcd(nil)
	//err = svr.StartEtcd(api.NewHandler(svr))
	if err != nil {
		log.Fatalf("server start etcd failed - %v", errors.Trace(err))
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go svr.Run()

	time.Sleep(5 * time.Second)

	//svr.BootstrapCluster(nil)

	sig := <-sc
	//svr.Close()
	log.Infof("Got signal [%d] to exit.", sig)
	switch sig {
	case syscall.SIGTERM:
		os.Exit(0)
	default:
		os.Exit(1)
	}
}
