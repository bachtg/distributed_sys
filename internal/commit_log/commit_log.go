package commit_log

import (
	"errors"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type CommitLog struct {
	logger        *slog.Logger
	mu            sync.RWMutex
	segments      []*Segment
	activeSegment *Segment
	dir           string
}

func NewCommitLog(logger *slog.Logger, dir string) (*CommitLog, error) {
	commitLog := &CommitLog{
		logger:   logger,
		segments: make([]*Segment, 0),
		dir:      dir,
	}

	if err := commitLog.loadSegments(); err != nil {
		return nil, err
	}

	if len(commitLog.segments) == 0 {
		segment, err := NewSegment(0, dir)
		if err != nil {
			return nil, err
		}
		commitLog.segments = append(commitLog.segments, segment)
		commitLog.activeSegment = segment
	}

	return commitLog, nil
}

func (commitLog *CommitLog) loadSegments() error {
	if err := os.MkdirAll(commitLog.dir, 0755); err != nil {
		return err
	}

	commitLog.logger.Info("Loading segments from directory", "dir", commitLog.dir)

	files, err := os.ReadDir(commitLog.dir)
	if err != nil {
		return err
	}

	var baseOffsets []int64
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".log") {
			continue
		}

		offsetStr := strings.TrimSuffix(file.Name(), ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue
		}
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, offset := range baseOffsets {
		segment, err := NewSegment(offset, commitLog.dir)
		if err != nil {
			return err
		}

		commitLog.segments = append(commitLog.segments, segment)
		commitLog.activeSegment = segment
	}

	return nil
}

func (commitLog *CommitLog) AppendAtOffset(entry LogEntry) error {
	commitLog.logger.Info("CommitLog | AppendAtOffset | Start processing", "offset", entry.Offset)
	defer commitLog.logger.Info("CommitLog | AppendAtOffset | Finished processing", "offset", entry.Offset)

	commitLog.mu.Lock()
	defer commitLog.mu.Unlock()

	// Check if need new segment
	if commitLog.activeSegment.baseOffset > entry.Offset {
		// Find or create appropriate segment
		var targetSegment *Segment
		for _, seg := range commitLog.segments {
			if seg.baseOffset <= entry.Offset {
				targetSegment = seg
			}
		}
		if targetSegment == nil {
			seg, err := NewSegment(entry.Offset, commitLog.dir)
			if err != nil {
				return err
			}
			commitLog.segments = append(commitLog.segments, seg)
			targetSegment = seg
		}
		return targetSegment.Append(entry)
	}

	if commitLog.activeSegment.IsFull() {
		if err := commitLog.activeSegment.Close(); err != nil {
			return err
		}

		newSegment, err := NewSegment(entry.Offset, commitLog.dir)
		if err != nil {
			return err
		}

		commitLog.segments = append(commitLog.segments, newSegment)
		commitLog.activeSegment = newSegment
	}

	return commitLog.activeSegment.Append(entry)
}

func (commitLog *CommitLog) Read(offset int64) (LogEntry, error) {
	commitLog.logger.Info("CommitLog | Read | Start processing", "offset", offset)
	defer commitLog.logger.Info("CommitLog | Read | Finished processing", "offset", offset)

	commitLog.mu.RLock()
	defer commitLog.mu.RUnlock()

	for _, segment := range commitLog.segments {
		entries, err := segment.Read()
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.Offset == offset {
				return entry, nil
			}
		}
	}

	return LogEntry{}, errors.New("offset not found")
}

func (commitLog *CommitLog) Close() error {
	commitLog.logger.Info("CommitLog | Close")

	commitLog.mu.Lock()
	defer commitLog.mu.Unlock()

	for _, segment := range commitLog.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}
