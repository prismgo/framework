package filesystem

import (
	"context"

	"github.com/prismgo/framework/exception"
)

func reportCleanupError(ctx context.Context, err error, operation string, fields map[string]any) {
	if err == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reportFields := map[string]any{
		"component": "filesystem",
		"operation": operation,
	}
	for key, value := range fields {
		reportFields[key] = value
	}
	exception.Report(ctx, err, reportFields)
}
