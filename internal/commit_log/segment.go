package commit_log

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const segmentMaxBytes = 10 * 1024 * 1024 // 10MB

type Segment struct {
	baseOffset int64
	file       *os.File
	writer     *bufio.Writer
	size       int64
	mu         sync.Mutex
}

func NewSegment(baseOffset int64, dir string) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	filename := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Segment{
		baseOffset: baseOffset,
		file:       file,
		writer:     bufio.NewWriter(file),
		size:       stat.Size(),
	}, nil
}

func (s *Segment) Append(entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	// Write length prefix (4 bytes) + data
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := s.writer.Write(lenBuf); err != nil {
		return err
	}
	if _, err := s.writer.Write(data); err != nil {
		return err
	}

	s.size += int64(4 + len(data))
	return s.writer.Flush()
}

func (s *Segment) Read() ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []LogEntry
	reader := bufio.NewReader(file)

	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		var entry LogEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *Segment) IsFull() bool {
	return s.size >= segmentMaxBytes
}
