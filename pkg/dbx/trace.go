//go:build sqlite_trace
// +build sqlite_trace

package dbx

import (
	"database/sql"
	"fmt"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"
)

var TraceCache = sync.Map{}

func traceCallback(info sqlite3.TraceInfo) int {
	// Not very readable but may be useful; uncomment next line in case of doubt:
	//fmt.Printf("Trace: %#v\n", info)

	if info.EventCode == 1 {
		var expandedText string
		if info.ExpandedSQL != "" {
			if info.ExpandedSQL == info.StmtOrTrigger {
				expandedText = " = exp"
			} else {
				expandedText = fmt.Sprintf(" expanded {%q}", info.ExpandedSQL)
			}
		}

		TraceCache.Store(info.StmtHandle, expandedText)
	}

	if (SlowQueryThresholdMs != 0) && (info.RunTimeNanosec == 0) {
		return 0
	}

	if info.EventCode == 2 {
		expandedText, _ := TraceCache.Load(info.StmtHandle)
		if expandedText == "" {
			return 0
		}
		TraceCache.Delete(info.StmtHandle)

		if info.RunTimeNanosec < (SlowQueryThresholdMs * int64(1000000)) {
			return 0
		}

		var runTimeText string
		const nanosPerMillisec = 1000000
		if info.RunTimeNanosec%nanosPerMillisec == 0 {
			runTimeText = fmt.Sprintf("; time %d ms", info.RunTimeNanosec/nanosPerMillisec)
		} else {
			// unexpected: better than millisecond resolution
			runTimeText = fmt.Sprintf("; time %d ns!!!", info.RunTimeNanosec)
		}

		var modeText string
		if info.AutoCommit {
			modeText = "-AC-"
		} else {
			modeText = "+Tx+"
		}

		fmt.Printf(">>> Slow Query Trace: ev %s conn 0x%x, stmt 0x%x {%q}%s%s\n",
			modeText, info.ConnHandle, info.StmtHandle,
			info.StmtOrTrigger, expandedText,
			runTimeText)

	}

	return 0
}

func init() {
	eventMask := sqlite3.TraceStmt | sqlite3.TraceProfile | sqlite3.TraceRow | sqlite3.TraceClose
	sql.Register("sqlite3_tracing",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.SetFileControlInt("", sqlite3.SQLITE_FCNTL_MMAP_SIZE, SQLiteMMapSize)
				if err != nil {
					return err
				}
				if SlowQueryThresholdMs < 0 {
					return nil
				}

				return conn.SetTrace(&sqlite3.TraceConfig{
					Callback:        traceCallback,
					EventMask:       eventMask,
					WantExpandedSQL: true,
				})
			},
		},
	)
	SQLiteDriver = "sqlite3_tracing"
}
