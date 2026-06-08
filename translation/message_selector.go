package translation

import (
	"strconv"
	"strings"
)

type MessageSelector struct{}

func NewMessageSelector() *MessageSelector {
	return &MessageSelector{}
}

func (s *MessageSelector) Select(message string, number any, locale string) string {
	if !strings.Contains(message, "|") {
		return message
	}

	segments := strings.Split(message, "|")
	idx := s.findIndex(segments, number, locale)

	if idx < 0 || idx >= len(segments) {
		return message
	}

	return strings.TrimSpace(s.stripIntervalPrefix(segments[idx]))
}

func (s *MessageSelector) stripIntervalPrefix(segment string) string {
	segment = strings.TrimSpace(segment)
	if len(segment) == 0 {
		return segment
	}

	if segment[0] == '{' {
		if idx := strings.IndexByte(segment, '}'); idx >= 0 {
			return strings.TrimSpace(segment[idx+1:])
		}
	}
	if segment[0] == '[' {
		if idx := strings.IndexByte(segment, ']'); idx >= 0 {
			return strings.TrimSpace(segment[idx+1:])
		}
	}
	return segment
}

func (s *MessageSelector) findIndex(segments []string, number any, locale string) int {
	numberInt := s.toInt(number)

	hasExplicitInterval := false
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if len(segment) > 0 && (segment[0] == '{' || segment[0] == '[') {
			hasExplicitInterval = true
			break
		}
	}

	if !hasExplicitInterval {
		if numberInt == 0 && len(segments) > 0 {
			return 0
		}
		if numberInt == 1 && len(segments) > 1 {
			return 1
		}
		if numberInt >= 2 && len(segments) > 2 {
			return 2
		}
		if numberInt >= 2 && len(segments) == 2 {
			return 1
		}
		return -1
	}

	for i, segment := range segments {
		segment = strings.TrimSpace(segment)

		if len(segment) > 0 && segment[0] == '{' {
			interval := s.parseExactInterval(segment)
			if interval != nil && interval.Match(numberInt) {
				return i
			}
		} else if len(segment) > 0 && segment[0] == '[' {
			interval := s.parseRangeInterval(segment)
			if interval != nil && interval.Match(numberInt) {
				return i
			}
		}
	}

	return -1
}

type Interval struct {
	Start int
	End   int
}

func (i *Interval) Match(n int) bool {
	if i.End == -1 {
		return n >= i.Start
	}
	return n >= i.Start && n <= i.End
}

func (s *MessageSelector) parseExactInterval(segment string) *Interval {
	segment = strings.TrimSpace(segment)

	if !strings.HasPrefix(segment, "{") {
		return nil
	}

	closeIdx := strings.IndexByte(segment, '}')
	if closeIdx < 0 {
		return nil
	}

	numStr := strings.TrimSpace(segment[1:closeIdx])
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return nil
	}

	return &Interval{Start: n, End: n}
}

func (s *MessageSelector) parseRangeInterval(segment string) *Interval {
	segment = strings.TrimSpace(segment)

	if !strings.HasPrefix(segment, "[") {
		return nil
	}

	closeIdx := strings.IndexByte(segment, ']')
	if closeIdx < 0 {
		return nil
	}

	inner := strings.TrimSpace(segment[1:closeIdx])

	parts := strings.SplitN(inner, ",", 2)
	if len(parts) != 2 {
		start, err := strconv.Atoi(strings.TrimSpace(inner))
		if err != nil {
			return nil
		}
		return &Interval{Start: start, End: start}
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	start := 0
	hasStart := false
	end := -1
	hasEnd := false

	if startStr != "" {
		if n, err := strconv.Atoi(startStr); err == nil {
			start = n
			hasStart = true
		}
	}

	if endStr == "*" {
		hasEnd = true
		end = -1
	} else if endStr != "" {
		if n, err := strconv.Atoi(endStr); err == nil {
			end = n
			hasEnd = true
		}
	}

	if hasStart && hasEnd {
		return &Interval{Start: start, End: end}
	}

	if hasStart && !hasEnd {
		return &Interval{Start: start, End: start}
	}

	if !hasStart && hasEnd {
		return &Interval{Start: 0, End: end}
	}

	return nil
}

func (s *MessageSelector) toInt(number any) int {
	switch v := number.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	default:
		return 0
	}
}
