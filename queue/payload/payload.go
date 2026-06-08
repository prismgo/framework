// Package payload 保存队列持久化 payload 的稳定 DTO 和 codec。
//
// 需求背景：queue transport driver、failed repository、batch/chain 元数据和自定义
// driver 都需要读写同一份 durable envelope。DTO 放在实现层 payload 包中，避免
// contracts/queue 承担具体存储格式。
package payload

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrCipherMissing = errors.New("queue payload: cipher is not configured")

// Payload 是已经由 queue payload codec 编码好的 Job payload 字节。
type Payload []byte

// MarshalJSON 保留 raw JSON 语义，避免 envelope.payload 被 encoding/json 编码为 base64。
func (p Payload) MarshalJSON() ([]byte, error) {
	raw := normalizeRawJSON(p)
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid raw JSON payload")
	}
	return raw, nil
}

// UnmarshalJSON 保留 JSON 源字节，供 worker 后续按 Job 类型反序列化。
func (p *Payload) UnmarshalJSON(data []byte) error {
	if p == nil {
		return nil
	}
	*p = append((*p)[:0], data...)
	return nil
}

// PendingJob 是链式任务中尚未投递的后续任务快照。
type PendingJob struct {
	Name           string   `json:"name"`
	Payload        Payload  `json:"payload"`
	MaxTries       int      `json:"max_tries,omitempty"`
	MaxExceptions  int      `json:"max_exceptions,omitempty"`
	TimeoutSec     int      `json:"timeout_sec,omitempty"`
	FailOnTimeout  bool     `json:"fail_on_timeout,omitempty"`
	Encrypted      bool     `json:"encrypted,omitempty"`
	BackoffSec     []int    `json:"backoff_sec,omitempty"`
	RetryUntil     int64    `json:"retry_until,omitempty"`
	Queue          string   `json:"queue,omitempty"`
	UniqueKey      string   `json:"unique_key,omitempty"`
	UniqueForSec   int      `json:"unique_for_sec,omitempty"`
	UniqueVia      string   `json:"unique_via,omitempty"`
	UniqueUntil    bool     `json:"unique_until_processing,omitempty"`
	DebounceKey    string   `json:"debounce_key,omitempty"`
	DebounceForSec int      `json:"debounce_for_sec,omitempty"`
	DebounceVia    string   `json:"debounce_via,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Silenced       bool     `json:"silenced,omitempty"`
}

// Envelope 是队列 transport 持久化和传输的任务信封。
type Envelope struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	Queue          string       `json:"queue"`
	Payload        Payload      `json:"payload"`
	Attempts       int          `json:"attempts"`
	Exceptions     int          `json:"exceptions,omitempty"`
	MaxTries       int          `json:"max_tries,omitempty"`
	MaxExceptions  int          `json:"max_exceptions,omitempty"`
	TimeoutSec     int          `json:"timeout_sec,omitempty"`
	FailOnTimeout  bool         `json:"fail_on_timeout,omitempty"`
	Encrypted      bool         `json:"encrypted,omitempty"`
	BackoffSec     []int        `json:"backoff_sec,omitempty"`
	RetryUntil     int64        `json:"retry_until,omitempty"`
	Chain          []PendingJob `json:"chain,omitempty"`
	BatchID        string       `json:"batch_id,omitempty"`
	UniqueKey      string       `json:"unique_key,omitempty"`
	UniqueForSec   int          `json:"unique_for_sec,omitempty"`
	UniqueVia      string       `json:"unique_via,omitempty"`
	UniqueUntil    bool         `json:"unique_until_processing,omitempty"`
	DebounceKey    string       `json:"debounce_key,omitempty"`
	DebounceForSec int          `json:"debounce_for_sec,omitempty"`
	DebounceVia    string       `json:"debounce_via,omitempty"`
	Tags           []string     `json:"tags,omitempty"`
	Silenced       bool         `json:"silenced,omitempty"`
	CreatedAt      int64        `json:"created_at"`
	AvailableAt    int64        `json:"available_at"`
	ReservedAt     int64        `json:"reserved_at,omitempty"`
}

// BatchStatus 保存批次执行进度。
type BatchStatus struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Total       int       `json:"total"`
	Pending     int       `json:"pending"`
	Processed   int       `json:"processed"`
	Failed      int       `json:"failed"`
	Cancelled   bool      `json:"cancelled"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
	CancelledAt time.Time `json:"cancelled_at,omitempty"`
}

// FailedJob 是 failed repository 保存的完整失败任务快照。
type FailedJob struct {
	ID         string    `json:"id"`
	Connection string    `json:"connection"`
	Queue      string    `json:"queue"`
	JobID      string    `json:"job_id"`
	JobName    string    `json:"job_name"`
	Envelope   Envelope  `json:"envelope"`
	Error      string    `json:"error"`
	FailedAt   time.Time `json:"failed_at"`
	Tags       []string  `json:"tags,omitempty"`
	Silenced   bool      `json:"silenced,omitempty"`
}

// MarshalEnvelope 编码 envelope，并在 JSON 模式下校验 raw payload 字段。
func MarshalEnvelope(env Envelope) ([]byte, error) {
	if err := validateEnvelopePayloads(env, "payload"); err != nil {
		return nil, err
	}
	return json.Marshal(env)
}

// UnmarshalEnvelope 解码 envelope。
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// MarshalFailedJob 编码 failed job 快照，并校验嵌套 envelope payload。
func MarshalFailedJob(failed FailedJob) ([]byte, error) {
	if err := validateEnvelopePayloads(failed.Envelope, "envelope.payload"); err != nil {
		return nil, err
	}
	return json.Marshal(failed)
}

// UnmarshalFailedJob 解码 failed job 快照。
func UnmarshalFailedJob(data []byte) (FailedJob, error) {
	var failed FailedJob
	if err := json.Unmarshal(data, &failed); err != nil {
		return FailedJob{}, err
	}
	return failed, nil
}

func validateEnvelopePayloads(env Envelope, rootPath string) error {
	if !json.Valid(normalizeRawJSON(env.Payload)) {
		return fmt.Errorf("%s: invalid raw JSON payload", rootPath)
	}
	for i, job := range env.Chain {
		if !json.Valid(normalizeRawJSON(job.Payload)) {
			return fmt.Errorf("chain[%d].payload: invalid raw JSON payload", i)
		}
	}
	return nil
}

func normalizeRawJSON(payload Payload) []byte {
	if len(payload) == 0 {
		return []byte("null")
	}
	return payload
}
