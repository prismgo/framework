package database

import (
	"testing"
)

func TestValidateCharset(t *testing.T) {
	tests := []struct {
		name    string
		charset string
		wantErr bool
	}{
		// 有效的 charset
		{"utf8mb4", "utf8mb4", false},
		{"utf8", "utf8", false},
		{"latin1", "latin1", false},
		{"ascii", "ascii", false},
		{"binary", "binary", false},
		{"utf8mb3", "utf8mb3", false},
		{"ucs2", "ucs2", false},
		{"utf16", "utf16", false},
		{"utf32", "utf32", false},
		{"big5", "big5", false},
		{"gbk", "gbk", false},
		{"gb2312", "gb2312", false},
		{"gb18030", "gb18030", false},
		{"euckr", "euckr", false},
		{"sjis", "sjis", false},
		{"cp932", "cp932", false},
		{"tis620", "tis620", false},
		{"koi8r", "koi8r", false},
		{"koi8u", "koi8u", false},
		{"cp1250", "cp1250", false},
		{"cp1251", "cp1251", false},
		{"cp1256", "cp1256", false},
		{"cp1257", "cp1257", false},
		{"cp850", "cp850", false},
		{"cp852", "cp852", false},
		{"cp866", "cp866", false},
		{"dec8", "dec8", false},
		{"hp8", "hp8", false},
		{"macce", "macce", false},
		{"macroman", "macroman", false},
		{"swe7", "swe7", false},
		{"ujis", "ujis", false},
		{"keybcs2", "keybcs2", false},
		{"latin2", "latin2", false},
		{"latin5", "latin5", false},
		{"latin7", "latin7", false},
		{"hebrew", "hebrew", false},
		{"greek", "greek", false},
		{"armscii8", "armscii8", false},
		{"geostd8", "geostd8", false},

		// 无效的 charset
		{"invalid charset", "invalid", true},
		{"empty charset", "", true},
		{"sql injection", "utf8mb4'; DROP TABLE users; --", true},

		// 大小写不敏感（应通过）
		{"UTF8MB4 uppercase", "UTF8MB4", false},
		{"Utf8mb4 mixed", "Utf8mb4", false},
		{"UTF8 uppercase", "UTF8", false},
		{"Latin1 mixed", "Latin1", false},
		{"BINARY uppercase", "BINARY", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCharset(tt.charset)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCharset(%q) error = %v, wantErr %v", tt.charset, err, tt.wantErr)
			}
		})
	}
}

func TestValidateCollation(t *testing.T) {
	tests := []struct {
		name      string
		charset   string
		collation string
		wantErr   bool
	}{
		// 有效的 collation（按 charset 前缀匹配）
		{"utf8mb4 with utf8mb4 collation", "utf8mb4", "utf8mb4_unicode_ci", false},
		{"utf8mb4 with utf8mb4 general", "utf8mb4", "utf8mb4_general_ci", false},
		{"utf8mb4 with utf8mb4 0900", "utf8mb4", "utf8mb4_0900_ai_ci", false},
		{"utf8 with utf8 collation", "utf8", "utf8_unicode_ci", false},
		{"utf8 with utf8 general", "utf8", "utf8_general_ci", false},
		{"latin1 with latin1 collation", "latin1", "latin1_swedish_ci", false},
		{"ascii with ascii collation", "ascii", "ascii_general_ci", false},
		{"binary with binary collation", "binary", "binary", false},
		{"gbk with gbk collation", "gbk", "gbk_chinese_ci", false},
		{"big5 with big5 collation", "big5", "big5_chinese_ci", false},

		// 无效的 collation（不匹配 charset 前缀）
		{"utf8mb4 with utf8 collation", "utf8mb4", "utf8_unicode_ci", true},
		{"utf8 with utf8mb4 collation", "utf8", "utf8mb4_unicode_ci", true},
		{"latin1 with utf8 collation", "latin1", "utf8_unicode_ci", true},

		// 空值
		{"empty collation", "utf8mb4", "", true},

		// SQL 注入
		{"sql injection", "utf8mb4", "utf8mb4_unicode_ci'; DROP TABLE users; --", true},

		// 大小写不敏感（应通过）
		{"UTF8MB4_UNICODE_CI uppercase", "UTF8MB4", "UTF8MB4_UNICODE_CI", false},
		{"Utf8mb4_Unicode_Ci mixed", "Utf8mb4", "Utf8mb4_Unicode_Ci", false},
		{"UTF8_GENERAL_CI uppercase", "UTF8", "UTF8_GENERAL_CI", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCollation(tt.charset, tt.collation)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCollation(%q, %q) error = %v, wantErr %v", tt.charset, tt.collation, err, tt.wantErr)
			}
		})
	}
}
