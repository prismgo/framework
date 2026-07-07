package database

import (
	"testing"
)

func TestValidateSqlMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		// 有效的 SQL mode
		{"ONLY_FULL_GROUP_BY", "ONLY_FULL_GROUP_BY", false},
		{"STRICT_TRANS_TABLES", "STRICT_TRANS_TABLES", false},
		{"NO_ZERO_IN_DATE", "NO_ZERO_IN_DATE", false},
		{"NO_ZERO_DATE", "NO_ZERO_DATE", false},
		{"ERROR_FOR_DIVISION_BY_ZERO", "ERROR_FOR_DIVISION_BY_ZERO", false},
		{"NO_AUTO_CREATE_USER", "NO_AUTO_CREATE_USER", false},
		{"NO_ENGINE_SUBSTITUTION", "NO_ENGINE_SUBSTITUTION", false},
		{"ANSI", "ANSI", false},
		{"TRADITIONAL", "TRADITIONAL", false},
		{"ALLOW_INVALID_DATES", "ALLOW_INVALID_DATES", false},

		// 无效的 SQL mode
		{"invalid mode", "INVALID_MODE", true},
		{"empty mode", "", true},
		{"sql injection", "STRICT_TRANS_TABLES'; DROP TABLE users; --", true},

		// 大小写不敏感（应通过）
		{"strict_trans_tables lowercase", "strict_trans_tables", false},
		{"no_zero_date lowercase", "no_zero_date", false},
		{"ansi lowercase", "ansi", false},
		{"Strict_Trans_Tables mixed", "Strict_Trans_Tables", false},
		{"Only_Full_Group_By mixed", "Only_Full_Group_By", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSqlMode(tt.mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSqlMode(%q) error = %v, wantErr %v", tt.mode, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSqlModes(t *testing.T) {
	tests := []struct {
		name    string
		modes   []string
		wantErr bool
	}{
		// 有效的 modes 组合
		{"strict mode set", []string{"ONLY_FULL_GROUP_BY", "STRICT_TRANS_TABLES", "NO_ZERO_IN_DATE"}, false},
		{"single mode", []string{"NO_ENGINE_SUBSTITUTION"}, false},
		{"empty list", []string{}, false},

		// 包含无效 mode
		{"contains invalid", []string{"STRICT_TRANS_TABLES", "INVALID_MODE"}, true},
		{"contains empty", []string{"STRICT_TRANS_TABLES", ""}, true},
		{"contains injection", []string{"STRICT_TRANS_TABLES', 'NO_ZERO_DATE"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSqlModes(tt.modes)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSqlModes(%v) error = %v, wantErr %v", tt.modes, err, tt.wantErr)
			}
		})
	}
}
