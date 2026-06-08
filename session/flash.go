package session

// Flash 写入当前请求可读、下一次请求仍可读的临时值。
//
// 参数 key 是临时数据键；value 是待保存值。调用后 key 会进入 NewFlash，Save 时推进到下一轮 OldFlash。
func (s *Store) Flash(key string, value any) {
	s.Put(key, value)
	s.payload.NewFlash = appendUnique(s.payload.NewFlash, key)
	s.payload.OldFlash = removeString(s.payload.OldFlash, key)
}

// Now 写入仅当前请求可读的临时值。
//
// 设计思路：Now 写入后直接放入 OldFlash，当前请求结束保存时会被清理，不进入下一次请求。
func (s *Store) Now(key string, value any) {
	s.Put(key, value)
	s.payload.OldFlash = appendUnique(s.payload.OldFlash, key)
	s.payload.NewFlash = removeString(s.payload.NewFlash, key)
}

// Reflash 将所有当前 flash 数据再延长一个请求周期。
func (s *Store) Reflash() {
	s.payload.NewFlash = uniqueStrings(append(s.payload.NewFlash, s.payload.OldFlash...))
	s.payload.OldFlash = nil
	s.markDirty()
}

// Keep 只延长指定 flash key 的生命周期。
//
// 参数 keys 是需要保留的 flash 键；不在 OldFlash 中的 key 会被忽略，避免误把普通 session 数据提升为 flash。
func (s *Store) Keep(keys ...string) {
	for _, key := range keys {
		if containsString(s.payload.OldFlash, key) {
			s.payload.NewFlash = appendUnique(s.payload.NewFlash, key)
			s.payload.OldFlash = removeString(s.payload.OldFlash, key)
		}
	}
	s.markDirty()
}

func appendUnique(items []string, item string) []string {
	if containsString(items, item) {
		return items
	}
	return append(items, item)
}

func uniqueStrings(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = appendUnique(out, item)
	}
	return out
}

func removeString(items []string, target string) []string {
	out := items[:0]
	for _, item := range items {
		if item != target {
			out = append(out, item)
		}
	}
	return out
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
