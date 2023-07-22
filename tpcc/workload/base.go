package workload

import (
	"cabinet/tpcc/util"
	"context"
	"database/sql"
	"math/rand"
)

// TpcState saves state for each thread
type TpcState struct {
	DB   *sql.DB
	Conn *sql.Conn

	R *rand.Rand

	Buf *util.BufAllocator
}

func (t *TpcState) RefreshConn(ctx context.Context) error {
	if t.Conn != nil {
		t.Conn.Close()
	}
	conn, err := t.DB.Conn(ctx)
	if err != nil {
		return err
	}
	t.Conn = conn
	return nil
}

// NewTpcState creates a base TpcState
func NewTpcState(ctx context.Context, db *sql.DB, seed int64) *TpcState {
	var conn *sql.Conn
	var err error
	if db != nil {
		conn, err = db.Conn(ctx)
		if err != nil {
			panic(err.Error())
		}
	}

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r := rand.New(rand.NewSource(seed))

	s := &TpcState{
		DB:   db,
		Conn: conn,
		R:    r,
		Buf:  util.NewBufAllocator(),
	}
	return s
}
