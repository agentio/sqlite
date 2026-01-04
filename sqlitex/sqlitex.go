package sqlitex

import (
	"fmt"

	"github.com/agentio/sqlite/sqliteh"
	"github.com/agentio/sqlite/sqlitepool"
)

type ExecOptions struct {
	Args       []any
	ResultFunc func(stmt sqliteh.Stmt) error
}

func ExecuteTransient(conn *sqlitepool.Rx, query string, options *ExecOptions) error {
	stmt := conn.Prepare(query)
	if options != nil {
		for i, a := range options.Args {
			switch v := a.(type) {
			case nil:
				stmt.BindNull(i + 1)
			case int64:
				stmt.BindInt64(i+1, v)
			case float64:
				stmt.BindDouble(i+1, v)
			case string:
				stmt.BindText64(i+1, v)
			case []byte:
				stmt.BindBlob64(i+1, v)
			default:
				return fmt.Errorf("unhandled type %T (fixme)", a)
			}
		}
	}
	running := true
	for running {
		if row, err := stmt.Step(nil); err != nil {
			return err
		} else if !row {
			running = false
		} else if options != nil && options.ResultFunc != nil {
			err = options.ResultFunc(stmt)
			if err != nil {
				return err
			}
		}
	}
	return stmt.Reset()
}
