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

	// 第一步：尝试用显式条件（区间/精确值）匹配
	if value := s.extract(segments, number); value != "" {
		return value
	}

	// 第二步：剥离所有条件前缀，只保留文本
	stripped := s.stripConditions(segments)

	// 第三步：根据 locale 的复数规则计算索引
	pluralIdx := s.getPluralIndex(locale, s.toInt(number))

	if len(stripped) == 1 || pluralIdx >= len(stripped) {
		return strings.TrimSpace(stripped[0])
	}

	return strings.TrimSpace(stripped[pluralIdx])
}

// extract 尝试用显式区间条件匹配 segments，成功返回匹配段文本（已剥离前缀），失败返回空字符串。
func (s *MessageSelector) extract(segments []string, number any) string {
	numberInt := s.toInt(number)

	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if len(segment) == 0 {
			continue
		}

		if segment[0] == '{' || segment[0] == '[' {
			var interval *Interval
			if segment[0] == '{' {
				interval = s.parseExactInterval(segment)
			} else {
				interval = s.parseRangeInterval(segment)
			}
			if interval != nil && interval.Match(numberInt) {
				return strings.TrimSpace(s.stripIntervalPrefix(segment))
			}
		}
	}
	return ""
}

// stripConditions 剥离所有 segment 的区间前缀，只保留文本部分。
func (s *MessageSelector) stripConditions(segments []string) []string {
	result := make([]string, len(segments))
	for i, segment := range segments {
		result[i] = s.stripIntervalPrefix(segment)
	}
	return result
}

// getPluralIndex 根据 locale 的语法规则返回复数索引。
// 完全对齐 Laravel 13 Illuminate\Translation\MessageSelector::getPluralIndex。
// 支持区域变体 locale（如 en_US、zh_CN），采用精确匹配策略，与 Laravel 保持一致。
func (s *MessageSelector) getPluralIndex(locale string, number int) int {
	switch locale {
	// 无复数形式：始终返回 0
	case "az", "bo", "dz", "id", "ja", "jv", "ka", "km", "kn", "ko", "ms",
		"my", "th", "tr", "vi", "zh", "yo",
		"zh_CN", "zh_TW", "zh_HK", "zh_SG":
		return 0

	// 两种复数形式：number == 1 → 0，否则 → 1
	case "af", "bn", "bg", "ca", "da", "de", "el", "en", "eo", "es", "et",
		"eu", "fa", "fi", "fo", "fur", "fy", "gl", "gu", "ha", "he", "hu",
		"is", "it", "ku", "lb", "ml", "mn", "mr", "nah", "nb", "ne", "nl",
		"nn", "no", "om", "or", "pa", "pap", "ps", "pt", "so", "sq", "sv",
		"sw", "ta", "te", "tk", "ur", "zu",
		"en_US", "en_GB", "en_AU", "en_CA", "en_NZ", "en_IE", "en_ZA", "en_PH",
		"de_DE", "de_AT", "de_CH",
		"es_ES", "es_MX", "es_AR", "es_CO", "es_CL", "es_PE", "es_VE", "es_EC",
		"pt_PT", "pt_BR":
		if number == 1 {
			return 0
		}
		return 1

	// 两种复数形式：number == 0 或 1 → 0，否则 → 1
	case "am", "bh", "fil", "fr", "gun", "hi", "hy", "ln", "mg", "nso",
		"ti", "wa", "xbr",
		"fr_FR", "fr_CA", "fr_BE", "fr_CH", "fr_LU":
		if number == 0 || number == 1 {
			return 0
		}
		return 1

	// 三种复数形式（俄语规则）
	case "be", "bs", "hr", "ru", "sr", "uk",
		"ru_RU", "ru_UA":
		rem10 := number % 10
		rem100 := number % 100
		if rem10 == 1 && rem100 != 11 {
			return 0
		}
		if rem10 >= 2 && rem10 <= 4 && (rem100 < 12 || rem100 > 14) {
			return 1
		}
		return 2

	// 三种复数形式（捷克语规则）
	case "cs", "sk":
		if number == 1 {
			return 0
		}
		if number >= 2 && number <= 4 {
			return 1
		}
		return 2

	// 三种复数形式（爱尔兰语规则）
	case "ga":
		if number == 1 {
			return 0
		}
		if number == 2 {
			return 1
		}
		return 2

	// 三种复数形式（立陶宛语规则）
	case "lt":
		rem10 := number % 10
		rem100 := number % 100
		if rem10 == 1 && rem100 != 11 {
			return 0
		}
		if rem10 >= 2 && (rem100 < 10 || rem100 >= 20) {
			return 1
		}
		return 2

	// 四种复数形式（斯洛文尼亚语规则）
	case "sl":
		rem100 := number % 100
		if rem100 == 1 {
			return 0
		}
		if rem100 == 2 {
			return 1
		}
		if rem100 == 3 || rem100 == 4 {
			return 2
		}
		return 3

	// 两种复数形式（马其顿语规则）
	case "mk":
		rem10 := number % 10
		if rem10 == 1 {
			return 0
		}
		return 1

	// 四种复数形式（马耳他语规则）
	case "mt":
		rem100 := number % 100
		if number == 1 {
			return 0
		}
		if number == 0 || (rem100 > 1 && rem100 < 11) {
			return 1
		}
		if rem100 > 10 && rem100 < 20 {
			return 2
		}
		return 3

	// 三种复数形式（拉脱维亚语规则）
	case "lv":
		if number == 0 {
			return 0
		}
		rem10 := number % 10
		rem100 := number % 100
		if rem10 == 1 && rem100 != 11 {
			return 1
		}
		return 2

	// 三种复数形式（波兰语规则）
	case "pl":
		if number == 1 {
			return 0
		}
		rem10 := number % 10
		rem100 := number % 100
		if rem10 >= 2 && rem10 <= 4 && (rem100 < 12 || rem100 > 14) {
			return 1
		}
		return 2

	// 四种复数形式（威尔士语规则）
	case "cy":
		if number == 1 {
			return 0
		}
		if number == 2 {
			return 1
		}
		if number == 8 || number == 11 {
			return 2
		}
		return 3

	// 三种复数形式（罗马尼亚语规则）
	case "ro":
		if number == 1 {
			return 0
		}
		rem100 := number % 100
		if number == 0 || (rem100 > 0 && rem100 < 20) {
			return 1
		}
		return 2

	// 六种复数形式（阿拉伯语规则）
	case "ar", "ar_SA", "ar_EG", "ar_AE", "ar_JO", "ar_LB", "ar_MA", "ar_DZ",
		"ar_BH", "ar_IQ", "ar_KW", "ar_LY", "ar_OM", "ar_QA", "ar_SD", "ar_SY",
		"ar_TN", "ar_YE":
		if number == 0 {
			return 0
		}
		if number == 1 {
			return 1
		}
		if number == 2 {
			return 2
		}
		rem100 := number % 100
		if rem100 >= 3 && rem100 <= 10 {
			return 3
		}
		if rem100 >= 11 && rem100 <= 99 {
			return 4
		}
		return 5

	// 默认：返回 0
	default:
		return 0
	}
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
		return &Interval{Start: start, End: -1}
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
