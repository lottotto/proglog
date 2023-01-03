// _testパッケージはしないでおく。
package discovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	. "github.com/lottotto/proglog/internal/discovery"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	// 3つのメンバーを追加したので、その状態になっていることを検証している。joinsには初回を除き2回ジョインして、現在のメンバーは3つ、いなくなったのは0
	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && len(m[0].Members()) == 3 && len(handler.leaves) == 0
	}, 3*time.Second, 250*time.Millisecond)

	// クラスタからの解脱を実施してエラーがないかどうかを確認
	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && len(m[0].Members()) == 3 && m[0].Members()[2].Status == serf.StatusLeft && len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)

	// id:2のノードが抜けたことを確認する。(channelから受け取る)
	require.Equal(t, "2", <-handler.leaves)

}

//ヘルパー関数
func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1) // 1個の空きポート番号を含む[]intを返す
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}
	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}
	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

// interfaceでもないのにerをつけるのはおかしい。joins,leavesはそれぞれイベントが何回起きたのか、どのサーバで起きたのかをおしえてくれる
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		// joinsが空っぽじゃ無かったら、channelに書きのmapオブジェクトをぶち込む(チャンネルにはサイズの考え方があるはず。)
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
