package domain

import "time"

type LogEntry struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
}
