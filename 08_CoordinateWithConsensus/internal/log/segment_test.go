package log

import (
	"io"
	"os"
	"testing"

	api "github.com/lottotto/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// セグメントにレコードを追加し、同じレコードを読み出し、最終的にストアとインデックスの両方に指定された最大サイズに達するのかをテストする。

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.Remove(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	// 初期ロード時はEOFのため、baseOffset = NextOffsetになるはず
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// インデックスが最大
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	p, _ := proto.Marshal(want)
	c.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4
	c.Segment.MaxIndexBytes = 1024

	// 既存のセグメントを再構築
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// ストアが最大
	require.True(t, s.IsMaxed())

	require.NoError(t, s.Remove())
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())

}