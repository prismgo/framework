package translation

import "testing"

func TestMessageSelectorSelectSimple(t *testing.T) {
	s := NewMessageSelector()

	result := s.Select("Hello", 5, "en")
	if result != "Hello" {
		t.Errorf("Select = %v, want Hello", result)
	}
}

func TestMessageSelectorSelectSimplePipe(t *testing.T) {
	s := NewMessageSelector()

	// Laravel 英语规则：1→0, 其他→1（只有两种复数形式）
	result := s.Select("one|two|three", 0, "en")
	if result != "two" {
		t.Errorf("Select(0) = %v, want two", result)
	}

	result = s.Select("one|two|three", 1, "en")
	if result != "one" {
		t.Errorf("Select(1) = %v, want one", result)
	}

	result = s.Select("one|two|three", 5, "en")
	if result != "two" {
		t.Errorf("Select(5) = %v, want two", result)
	}
}

func TestMessageSelectorSelectZeroIndex(t *testing.T) {
	s := NewMessageSelector()

	// Laravel 英语规则：1→0 (singular), 其他→1 (plural)
	// 3 段式翻译必须用显式区间才能精确控制
	result := s.Select("{0} none|{1} one|[2,*] more", 0, "en")
	if result != "none" {
		t.Errorf("Select(0) = %v, want none", result)
	}

	result = s.Select("{0} none|{1} one|[2,*] more", 1, "en")
	if result != "one" {
		t.Errorf("Select(1) = %v, want one", result)
	}

	result = s.Select("{0} none|{1} one|[2,*] more", 5, "en")
	if result != "more" {
		t.Errorf("Select(5) = %v, want more", result)
	}
}

func TestMessageSelectorSelectTwoOptions(t *testing.T) {
	s := NewMessageSelector()

	// Laravel 英语规则：1→0 (singular), 其他→1 (plural)
	result := s.Select("one item|many items", 0, "en")
	if result != "many items" {
		t.Errorf("Select(0) = %v, want many items", result)
	}

	result = s.Select("one item|many items", 1, "en")
	if result != "one item" {
		t.Errorf("Select(1) = %v, want one item", result)
	}

	result = s.Select("one item|many items", 5, "en")
	if result != "many items" {
		t.Errorf("Select(5) = %v, want many items", result)
	}
}

func TestMessageSelectorSelectNoMatch(t *testing.T) {
	s := NewMessageSelector()

	result := s.Select("one|two", 100, "en")
	if result != "two" {
		t.Errorf("Select = %v, want two", result)
	}
}

func TestMessageSelectorToInt(t *testing.T) {
	s := NewMessageSelector()

	tests := []struct {
		input    any
		expected int
	}{
		{int(5), 5},
		{int64(10), 10},
		{int32(3), 3},
		{float64(7.9), 7},
		{float32(4.2), 4},
		{"15", 15},
		{"invalid", 0},
		{nil, 0},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := s.toInt(tt.input)
			if result != tt.expected {
				t.Errorf("toInt(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMessageSelectorIntervalParsing(t *testing.T) {
	s := NewMessageSelector()

	tests := []struct {
		name     string
		message  string
		number   int
		expected string
	}{
		{"exact_zero", "{0} No items|{1} One item|[2,*] Many items", 0, "No items"},
		{"exact_one", "{0} No items|{1} One item|[2,*] Many items", 1, "One item"},
		{"range", "{0} No items|{1} One item|[2,*] Many items", 5, "Many items"},
		{"range_end", "[1,10] In range|[11,*] Out of range", 5, "In range"},
		{"range_star_end", "[1,10] In range|[11,*] Out of range", 15, "Out of range"},
		{"empty_before", "[0,0] Zero|[1,*] Other", 0, "Zero"},
		{"empty_before_other", "[0,0] Zero|[1,*] Other", 3, "Other"},
		{"star_start", "[*,5] Few|[6,*] Many", 3, "Few"},
		{"star_start_many", "[*,5] Few|[6,*] Many", 7, "Many"},
		{"single_number_range", "[5] Exact|default", 5, "Exact"},
		{"single_number_range_no_match", "[5] Exact|default", 3, "default"},
		{"open_end_range", "[5,] Five or more|default", 5, "Five or more"},
		{"open_end_range_match", "[5,] Five or more|default", 10, "Five or more"},
		{"open_end_range_no_match", "[5,] Five or more|default", 3, "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.Select(tt.message, tt.number, "en")
			if result != tt.expected {
				t.Errorf("Select = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMessageSelectorNoPipe(t *testing.T) {
	s := NewMessageSelector()

	result := s.Select("Simple message", 1, "en")
	if result != "Simple message" {
		t.Errorf("Select = %v, want Simple message", result)
	}
}

func TestMessageSelectorMatchInterval(t *testing.T) {
	tests := []struct {
		name     string
		start    int
		end      int
		number   int
		expected bool
	}{
		{"match_exact", 5, 5, 5, true},
		{"no_match_exact", 5, 5, 3, false},
		{"match_range", 1, 10, 5, true},
		{"no_match_range", 1, 10, 11, false},
		{"match_unbounded", 5, -1, 100, true},
		{"no_match_unbounded", 5, -1, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval := &Interval{Start: tt.start, End: tt.end}
			result := interval.Match(tt.number)
			if result != tt.expected {
				t.Errorf("Interval{%d,%d}.Match(%d) = %v, want %v", tt.start, tt.end, tt.number, result, tt.expected)
			}
		})
	}
}

func TestMessageSelectorGetPluralIndex(t *testing.T) {
	s := NewMessageSelector()

	tests := []struct {
		name     string
		locale   string
		number   int
		expected int
	}{
		// 英语：1→0, 其他→1（只有两种复数形式）
		{"en_zero", "en", 0, 1},
		{"en_one", "en", 1, 0},
		{"en_two", "en", 2, 1},
		{"en_many", "en", 10, 1},

		// 法语：0 或 1→0, >=2→1
		{"fr_zero", "fr", 0, 0},
		{"fr_one", "fr", 1, 0},
		{"fr_two", "fr", 2, 1},
		{"fr_many", "fr", 10, 1},

		// 俄语：复杂规则
		// 1, 21, 31... → 0
		// 2-4, 22-24, 32-34... → 1
		// 0, 5-20, 25-30, 35-40... → 2
		{"ru_one", "ru", 1, 0},
		{"ru_twenty_one", "ru", 21, 0},
		{"ru_two", "ru", 2, 1},
		{"ru_three", "ru", 3, 1},
		{"ru_four", "ru", 4, 1},
		{"ru_twenty_two", "ru", 22, 1},
		{"ru_zero", "ru", 0, 2},
		{"ru_five", "ru", 5, 2},
		{"ru_ten", "ru", 10, 2},
		{"ru_twenty", "ru", 20, 2},

		// 阿拉伯语：6 种复数形式
		// 0 → 0, 1 → 1, 2 → 2, 3-10 → 3, 11-99 → 4, >=100 → 5
		{"ar_zero", "ar", 0, 0},
		{"ar_one", "ar", 1, 1},
		{"ar_two", "ar", 2, 2},
		{"ar_three", "ar", 3, 3},
		{"ar_ten", "ar", 10, 3},
		{"ar_eleven", "ar", 11, 4},
		{"ar_ninety_nine", "ar", 99, 4},
		{"ar_hundred", "ar", 100, 5},
		{"ar_thousand", "ar", 1000, 5},

		// 中文/日语：无复数，始终 0
		{"zh_zero", "zh", 0, 0},
		{"zh_one", "zh", 1, 0},
		{"zh_many", "zh", 100, 0},
		{"ja_zero", "ja", 0, 0},
		{"ja_one", "ja", 1, 0},
		{"ja_many", "ja", 100, 0},

		// 区域变体 locale（精确匹配，对齐 Laravel 13）
		{"en_US_one", "en_US", 1, 0},
		{"en_US_many", "en_US", 5, 1},
		{"en_GB_one", "en_GB", 1, 0},
		{"en_GB_many", "en_GB", 5, 1},
		{"zh_CN_zero", "zh_CN", 0, 0},
		{"zh_CN_one", "zh_CN", 1, 0},
		{"zh_CN_many", "zh_CN", 100, 0},
		{"zh_TW_zero", "zh_TW", 0, 0},
		{"zh_TW_one", "zh_TW", 1, 0},
		{"fr_FR_zero", "fr_FR", 0, 0},
		{"fr_FR_one", "fr_FR", 1, 0},
		{"fr_FR_two", "fr_FR", 2, 1},
		{"de_DE_one", "de_DE", 1, 0},
		{"de_DE_many", "de_DE", 5, 1},
		{"es_ES_one", "es_ES", 1, 0},
		{"es_ES_many", "es_ES", 5, 1},
		{"pt_BR_one", "pt_BR", 1, 0},
		{"pt_BR_many", "pt_BR", 5, 1},
		{"ru_RU_one", "ru_RU", 1, 0},
		{"ru_RU_two", "ru_RU", 2, 1},
		{"ru_RU_five", "ru_RU", 5, 2},
		{"ar_SA_zero", "ar_SA", 0, 0},
		{"ar_SA_one", "ar_SA", 1, 1},
		{"ar_SA_two", "ar_SA", 2, 2},
		{"ar_SA_three", "ar_SA", 3, 3},
		{"ar_SA_eleven", "ar_SA", 11, 4},
		{"ar_SA_hundred", "ar_SA", 100, 5},

		// 未知 locale 回退到默认规则（始终返回 0）
		{"unknown_zero", "xx", 0, 0},
		{"unknown_one", "xx", 1, 0},
		{"unknown_two", "xx", 2, 0},
		{"unknown_many", "xx", 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.getPluralIndex(tt.locale, tt.number)
			if result != tt.expected {
				t.Errorf("getPluralIndex(%q, %d) = %d, want %d", tt.locale, tt.number, result, tt.expected)
			}
		})
	}
}

func TestMessageSelectorPluralFallback(t *testing.T) {
	s := NewMessageSelector()

	// 显式区间全不匹配时，按复数索引回退
	tests := []struct {
		name     string
		message  string
		number   int
		locale   string
		expected string
	}{
		// 英语 2 段：1→index 0, 其他→index 1
		{"en_one_fallback", "singular|plural", 1, "en", "singular"},
		{"en_zero_fallback", "singular|plural", 0, "en", "plural"},
		{"en_many_fallback", "singular|plural", 5, "en", "plural"},

		// 法语 2 段：0 或 1→index 0, 其他→index 1
		{"fr_zero_fallback", "singular|plural", 0, "fr", "singular"},
		{"fr_one_fallback", "singular|plural", 1, "fr", "singular"},
		{"fr_many_fallback", "singular|plural", 5, "fr", "plural"},

		// 中文：始终 index 0
		{"zh_fallback", "first|second|third", 0, "zh", "first"},
		{"zh_fallback_one", "first|second|third", 1, "zh", "first"},
		{"zh_fallback_many", "first|second|third", 100, "zh", "first"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.Select(tt.message, tt.number, tt.locale)
			if result != tt.expected {
				t.Errorf("Select(%q, %d, %q) = %q, want %q", tt.message, tt.number, tt.locale, result, tt.expected)
			}
		})
	}
}
