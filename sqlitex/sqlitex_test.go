package sqlitex

import (
	"context"
	"testing"

	"github.com/agentio/sqlite/sqliteh"
	"github.com/agentio/sqlite/sqlitepool"
	"github.com/agentio/sqlite/sqlstats"
)

func TestExecute(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	initFn := func(db sqliteh.DB) error {
		return nil
	}
	tracer := &sqlstats.Tracer{}
	p, err := sqlitepool.NewPool("file:"+tempDir+"/sqlitex_test", 2, initFn, tracer)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := p.BeginTx(ctx, "test execute")
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteTransient(tx.Rx, "CREATE TABLE t (a TEXT, b INTEGER, c REAL, d BLOB, e INTEGER);", nil)
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteTransient(tx.Rx, "INSERT INTO t (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) RETURNING rowid;",
		&ExecOptions{
			Args: []any{"a1", int64(1), float64(-1.0), []byte("123"), nil},
			ResultFunc: func(stmt sqliteh.Stmt) error {
				if stmt.ColumnInt64(0) != 1 {
					t.Error("unexpected row id")
				}
				return nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteTransient(tx.Rx, "SELECT * FROM t WHERE rowid = ?;",
		&ExecOptions{
			Args: []any{int64(1)},
			ResultFunc: func(stmt sqliteh.Stmt) error {
				if stmt.ColumnText(0) != "a1" {
					t.Error("mismatch 1")
				}
				if stmt.ColumnInt64(1) != int64(1) {
					t.Error("mismatch 2")
				}
				if stmt.ColumnDouble(2) != float64(-1.0) {
					t.Error("mismatch 3")
				}
				if string(stmt.ColumnBlob(3)) != "123" {
					t.Error("mismatch 4")
				}
				if stmt.ColumnType(4) != sqliteh.SQLITE_NULL {
					t.Error("mismatch 5")
				}
				return nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}
}
