package redis

import "testing"

func TestRedisEventWrapperNames(t *testing.T) {
	cases := []struct {
		name string
		ev   interface{ Name() string }
		want string
	}{
		{name: "command executed", ev: CommandExecutedEvent{}, want: EventCommandExecuted},
		{name: "command failed", ev: CommandFailedEvent{}, want: EventCommandFailed},
		{name: "batch executed", ev: CommandBatchExecutedEvent{}, want: EventCommandBatchExecuted},
		{name: "batch failed", ev: CommandBatchFailedEvent{}, want: EventCommandBatchFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ev.Name(); got != tc.want {
				t.Fatalf("Name() = %q, want %q", got, tc.want)
			}
		})
	}
}
