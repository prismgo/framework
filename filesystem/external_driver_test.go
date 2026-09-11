package filesystem_test

import (
	"context"
	"errors"
	"testing"

	"github.com/prismgo/framework/filesystem"
)

func TestManagerRequiresExtensionForOSSDriver(t *testing.T) {
	manager, err := filesystem.NewManager(filesystem.Config{
		Default: "cloud",
		Disks: map[string]filesystem.DiskConfig{
			"cloud": {
				Driver: "oss",
				OSS: filesystem.OSSConfig{
					Bucket:   "example",
					Endpoint: "http://127.0.0.1:1",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewManager() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	err = manager.Default().Put(context.Background(), "probe.txt", "probe")
	if !errors.Is(err, filesystem.ErrUnsupportedDriver) {
		t.Fatalf("Put() error = %v, want ErrUnsupportedDriver", err)
	}
}
