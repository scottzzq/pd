package main

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
	"time"
)

var (
	etcdTimeout    = time.Second * 3
	endpoints      = []string{"127.0.0.1:2379"}
	dialTimeout    = 2 * time.Second
	requestTimeout = 2 * time.Second
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close() // make sure to close the client

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := cli.Get(ctx, "/pd/6396007400408189853/leader")
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}

}
