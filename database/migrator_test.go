package database

import "testing"

func TestTableOptionsUsesInnoDBForMySQL(t *testing.T) {
	want := "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	if got := TableOptions("mysql"); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if got := TableOptions(" MySQL "); got != want {
		t.Fatalf("expected trimmed %q, got %q", want, got)
	}
}

func TestTableOptionsSkipsNonMySQL(t *testing.T) {
	if got := TableOptions("sqlite"); got != "" {
		t.Fatalf("expected empty for sqlite, got %q", got)
	}
	if got := TableOptions(""); got != "" {
		t.Fatalf("expected empty for empty dialect, got %q", got)
	}
}

func TestSameColumns(t *testing.T) {
	tests := []struct {
		name        string
		left, right []string
		want        bool
	}{
		{"same composite primary key", []string{"tenant_id", "prefix_date"}, []string{"tenant_id", "prefix_date"}, true},
		{"legacy single primary key", []string{"prefix_date"}, []string{"tenant_id", "prefix_date"}, false},
		{"same columns different order", []string{"prefix_date", "tenant_id"}, []string{"tenant_id", "prefix_date"}, false},
		{"both empty", nil, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SameColumns(tt.left, tt.right); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestShouldAlterEngine(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		want   bool
	}{
		{"already innodb", "InnoDB", false},
		{"already innodb case insensitive", " innodb ", false},
		{"other engine", "MyISAM", true},
		{"empty engine", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldAlterEngine(tt.engine); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
