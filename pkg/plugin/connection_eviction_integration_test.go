//go:build integration

package plugin

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/stretchr/testify/require"
)

// The first socket completes the normal handshake and ping. Once armed, the
// proxy consumes its responses so that the driver's own read deadline fires.
type firstSocketResponse struct {
	dst     io.Writer
	discard atomic.Bool
	dropped atomic.Bool
}

func (w *firstSocketResponse) Write(p []byte) (int, error) {
	if w.discard.Load() {
		w.dropped.Store(true)
		return len(p), nil
	}
	return w.dst.Write(p)
}

func TestFirstBlockTimeoutEvictsConnection(t *testing.T) {
	ctx := t.Context()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	var first firstSocketResponse
	var accepted atomic.Int32
	target := net.JoinHostPort(getEnv("CLICKHOUSE_HOST", "localhost"), getEnv("CLICKHOUSE_PORT", "9000"))
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				client.Close()
				return
			}
			var dst io.Writer = client
			if accepted.Add(1) == 1 {
				first.dst = client
				dst = &first
			}
			go func() {
				defer server.Close()
				_, _ = io.Copy(server, client)
			}()
			go func() {
				defer client.Close()
				_, _ = io.Copy(dst, server)
			}()
		}
	}()

	settings, err := json.Marshal(Settings{
		Host:         "127.0.0.1",
		Port:         int64(listener.Addr().(*net.TCPAddr).Port),
		Username:     getEnv("CLICKHOUSE_USERNAME", "default"),
		QueryTimeout: "1",
		MaxOpenConns: "1",
		MaxIdleConns: "1",
	})
	require.NoError(t, err)
	db, err := (&Clickhouse{}).Connect(ctx, backend.DataSourceInstanceSettings{
		JSONData:                settings,
		DecryptedSecureJSONData: map[string]string{"password": getEnv("CLICKHOUSE_PASSWORD", "")},
	}, nil)
	require.NoError(t, err)
	defer db.Close()

	// Pin the connection so database/sql exposes the failure without retrying the query.
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	first.discard.Store(true)
	var value uint8
	err = conn.QueryRowContext(ctx, "SELECT toUInt8(1)").Scan(&value)
	require.Error(t, err)
	require.True(t, first.dropped.Load(), "the proxy must have withheld a server response")
	_ = conn.Close() // May already be closed by database/sql on ErrBadConn.
	require.Zero(t, db.Stats().OpenConnections, "the pool retained the timed-out connection: %v", err)
	require.ErrorIs(t, err, driver.ErrBadConn)

	rows, err := db.QueryContext(ctx, "SELECT toUInt8(2)")
	require.NoError(t, err)
	defer rows.Close()
	require.EqualValues(t, 2, accepted.Load())
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&value))
	require.EqualValues(t, 2, value)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}
