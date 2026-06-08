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

	result := s.Select("one|two|three", 0, "en")
	if result != "one" {
		t.Errorf("Select(0) = %v, want one", result)
	}

	result = s.Select("one|two|three", 1, "en")
	if result != "two" {
		t.Errorf("Select(1) = %v, want two", result)
	}

	result = s.Select("one|two|three", 5, "en")
	if result != "three" {
		t.Errorf("Select(5) = %v, want three", result)
	}
}

func TestMessageSelectorSelectZeroIndex(t *testing.T) {
	s := NewMessageSelector()

	result := s.Select("none|one|more", 0, "en")
	if result != "none" {
		t.Errorf("Select(0) = %v, want none", result)
	}

	result = s.Select("none|one|more", 1, "en")
	if result != "one" {
		t.Errorf("Select(1) = %v, want one", result)
	}

	result = s.Select("none|one|more", 5, "en")
	if result != "more" {
		t.Errorf("Select(5) = %v, want more", result)
	}
}

func TestMessageSelectorSelectTwoOptions(t *testing.T) {
	s := NewMessageSelector()

	result := s.Select("one item|many items", 0, "en")
	if result != "one item" {
		t.Errorf("Select(0) = %v, want one item", result)
	}

	result = s.Select("one item|many items", 1, "en")
	if result != "many items" {
		t.Errorf("Select(1) = %v, want many items", result)
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
		{"single_number_range_no_match", "[5] Exact|default", 3, "[5] Exact|default"},
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
