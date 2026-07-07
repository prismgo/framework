package database

import "testing"

func TestTableOptionsWithEngine(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  string
		want    string
	}{
		{"mysql with InnoDB", "mysql", "InnoDB", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		{"mysql with MyISAM", "mysql", "MyISAM", "ENGINE=MyISAM DEFAULT CHARSET=utf8mb4"},
		{"mysql with empty engine defaults to InnoDB", "mysql", "", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		{"mysql case insensitive", " MySQL ", "innodb", "ENGINE=innodb DEFAULT CHARSET=utf8mb4"},
		{"sqlite returns empty", "sqlite", "InnoDB", ""},
		{"empty dialect return empty", "", "InnoDB", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TableOptions(tt.dialect, tt.engine); got != tt.want {
				t.Fatalf("TableOptions(%q, %q) = %q, want %q", tt.dialect, tt.engine, got, tt.want)
			}
		})
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
