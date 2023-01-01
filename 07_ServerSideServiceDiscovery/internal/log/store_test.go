package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	// 一時的なファイルを現在のディレクトリに作成する
	f, err := os.CreateTemp("", "store_append_read_test")

	// assertパッケージと同じだが、失敗するとテストを終了する
	// 参考) https://github.com/stretchr/testify#require-package
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// 再びストアを作成し、ストアからの読み出しをテストすることで、サービスが再起動後に状態を回復することを検証する
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)

}

// testのためのヘルパー一つ目
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		// log.Printf("n: %v, pos: %v", n, pos)

		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)

		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		// log.Printf("offset: %v, b: %v, string(b): %v", off, b, string(b))
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)

		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		// bに読み取られて、それが書き込まれた文字と一致しているか
		require.Equal(t, write, b)

		require.Equal(t, int(size), n)
		off += int64(n)

	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	// 与えられたバイトをtempファイルに書き込む
	_, _, err = s.Append(write)
	require.NoError(t, err)
	// tempファイルを開き、サイズを獲得する。
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)
	// 一旦閉じる
	err = s.Close()
	require.NoError(t, err)

	// tempファイルを開き、サイズを獲得する
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	// もしafterよりbeforeSizeの方が大きかったらエラー
	// fmt.Printf("%d,%d", afterSize, beforeSize)
	// log.Printf("afterSize: %d, beforeSize: %d", afterSize, beforeSize)
	require.True(t, afterSize > beforeSize)

}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	//fi.Sizeはファイルの長さを戻す
	return f, fi.Size(), nil
}
