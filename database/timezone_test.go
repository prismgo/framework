package database

import (
	"testing"
)

func TestValidateTimezone(t *testing.T) {
	tests := []struct {
		name    string
		tz      string
		wantErr bool
	}{
		// 有效的 MySQL 命名时区
		{"UTC", "UTC", false},
		{"US/Eastern", "US/Eastern", false},
		{"US/Central", "US/Central", false},
		{"US/Mountain", "US/Mountain", false},
		{"US/Pacific", "US/Pacific", false},
		{"Asia/Shanghai", "Asia/Shanghai", false},
		{"Asia/Tokyo", "Asia/Tokyo", false},
		{"Europe/London", "Europe/London", false},
		{"Europe/Paris", "Europe/Paris", false},
		{"America/New_York", "America/New_York", false},
		{"America/Los_Angeles", "America/Los_Angeles", false},

		// 有效的偏移量格式（±HH:MM）
		{"+00:00", "+00:00", false},
		{"-05:00", "-05:00", false},
		{"+08:00", "+08:00", false},
		{"+12:00", "+12:00", false},
		{"-12:00", "-12:00", false},

		// 有效的偏移量格式（±H:MM，非标准但 MySQL 支持）
		{"+8:00 single digit", "+8:00", false},
		{"-5:00 single digit", "-5:00", false},
		{"+0:00 zero hour", "+0:00", false},
		{"+9:30 Adelaide", "+9:30", false},

		// SYSTEM 关键字（大小写不敏感）
		{"SYSTEM uppercase", "SYSTEM", false},
		{"system lowercase", "system", false},
		{"System mixed", "System", false},

		// 无效的时区
		{"empty", "", true},
		{"invalid timezone", "Invalid/Timezone", true},
		{"sql injection", "UTC'; DROP TABLE users; --", true},
		{"just plus", "+", true},
		{"just minus", "-", true},
		{"invalid offset format", "+25:00", true},
		{"invalid offset format 2", "+08:60", true},
		{"random string", "foobar", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTimezone(tt.tz)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTimezone(%q) error = %v, wantErr %v", tt.tz, err, tt.wantErr)
			}
		})
	}
}
