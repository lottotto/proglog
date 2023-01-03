package log_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	api "github.com/lottotto/proglog/api/v1"
	"github.com/lottotto/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	// 初期設定
	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := log.Config{}
		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		// 最初の時はブートストラップしてリーダになる。
		if i == 0 {
			config.Raft.Bootstrap = true
		}
		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)
		if i != 0 {
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		logs = append(logs, l)
	}

	// ログの書き込みなどのテスト
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	time.Sleep(50 * time.Millisecond)
	for _, record := range records {
		off, err := logs[0].Append(record)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				require.NoError(t, err)
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// リーダーがクラスタから離脱したサーバへのレプリケーションを停止する
	err := logs[0].Leave("1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// 1番目のノードにoffset1のものを問合せ。抜けた後だから特にないはず
	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	// 2番目のノードにoffset:1のものを問合せ。クラスタの中にいるはずだから、大丈夫
	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
