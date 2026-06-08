package make

import (
	"embed"
	"fmt"
	"path"
)

//go:embed stubs/*.stub
var builtInStubFS embed.FS

func readBuiltInStub(name string) (string, error) {
	content, err := builtInStubFS.ReadFile(path.Join("stubs", name))
	if err != nil {
		return "", fmt.Errorf("generator stub %q is not available", name)
	}
	return string(content), nil
}
