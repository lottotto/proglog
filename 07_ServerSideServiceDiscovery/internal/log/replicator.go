package log

import (
	"context"
	"sync"

	api "github.com/lottotto/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// gRPCを利用して、他のサーバに接続する。認証データはDialOptionsへ。serversはアドレスからチャネルへのマップ
// LocalserverのProduceメソッドを呼び出して、他のサーバから読み出したメッセージのコピーを保存する。要するにmembershipのハンドラ部分の実装
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		//既にレプリケーション実施済みのためスキップ
		return nil
	}
	// レプリケーション対象のサーバのリストに追加
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()
	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to comsume", addr)
		return
	}
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()
	// 見つかったらサーバからのログをストリームから呼び出し、ローカルサーバに書き込んでコピーを保存する
	// レプリケータがそのサーバのチャネルをクローズすると、ループから抜け出してgo routineは終了する
	for {
		select { // この書き方知らない
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}

}

// 離脱する時にそのアドレスのサーバリストから削除し、関連づけられたチャネルを閉じる。チャネルをクローズすることで、replicateメソッドにもクローズが伝わる
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil

}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)

}
