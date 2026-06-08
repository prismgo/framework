package session

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

const lockPollInterval = 10 * time.Millisecond

// fileLockManager 通过 O_CREATE|O_EXCL 锁文件实现跨进程独占锁。
//
// 需求背景：file session driver 需要保证同 session ID 的读、写、保存串行化，避免并发请求覆盖
// 或读取到半写入内容。
// 设计思路：每个 session ID 使用一个独立 lock 文件；创建成功代表持锁，释放时校验 token，
// 防止过期锁被其他请求重建后被旧持有者误删。
type fileLockManager struct {
	dir string
}

func newFileLockManager(dir string) (*fileLockManager, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &fileLockManager{dir: dir}, nil
}

func (m *fileLockManager) acquire(ctx context.Context, id string, ttl time.Duration, wait time.Duration) (Lock, error) {
	if ttl <= 0 {
		ttl = time.Duration(DefaultLockSeconds) * time.Second
	}
	if wait <= 0 {
		wait = time.Duration(DefaultLockWaitSeconds) * time.Second
	}
	deadline := time.Now().Add(wait)
	path := filepath.Join(m.dir, id+".lock")
	token := newSessionID()

	for {
		lock, err := tryCreateLockFile(path, token)
		if err == nil {
			return lock, nil
		}
		if !lockBusyError(err) {
			return nil, err
		}
		m.removeStale(path, ttl)
		if time.Now().After(deadline) {
			return nil, ErrLockTimeout
		}
		if err := sleepLockPoll(ctx, deadline); err != nil {
			return nil, err
		}
	}
}

// lockBusyError 判断创建锁文件失败是否代表“锁正被占用”。
//
// 需求背景：Windows 在锁文件刚被其他请求创建、读取或删除的竞争窗口内，可能返回
// permission denied，而不是 Unix 常见的 file exists。这里把该错误视为可等待竞争，
// 让同 session ID 的并发保存继续走轮询等待，避免把正常锁竞争暴露为业务保存失败。
func lockBusyError(err error) bool {
	return errors.Is(err, os.ErrExist) || errors.Is(err, os.ErrPermission)
}

func (m *fileLockManager) removeStale(path string, ttl time.Duration) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if time.Since(info.ModTime()) > ttl {
		_ = os.Remove(path)
	}
}

func tryCreateLockFile(path string, token string) (*fileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteString(token); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &fileLock{path: path, token: token, held: true}, nil
}

func sleepLockPoll(ctx context.Context, deadline time.Time) error {
	sleep := lockPollInterval
	if remaining := time.Until(deadline); remaining < sleep {
		sleep = remaining
	}
	if sleep <= 0 {
		return nil
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// fileLock 表示当前调用方持有的锁文件。
type fileLock struct {
	path  string
	token string
	held  bool
}

// Release 释放锁文件；若 token 不匹配，说明锁已过期并被其他请求重建，返回 ErrLockNotHeld。
func (l *fileLock) Release(_ context.Context) error {
	if l == nil || !l.held {
		return ErrLockNotHeld
	}
	raw, err := os.ReadFile(l.path)
	if err != nil {
		l.held = false
		if errors.Is(err, os.ErrNotExist) {
			return ErrLockNotHeld
		}
		return err
	}
	if string(raw) != l.token {
		l.held = false
		return ErrLockNotHeld
	}
	l.held = false
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
