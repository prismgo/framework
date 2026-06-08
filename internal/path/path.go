package path

import (
	"os"
	"path/filepath"
)

// Base 返回显式传入的应用根目录；未传入时从当前进程环境自动识别。
func Base(paths ...string) string {
	for _, path := range paths {
		if path != "" {
			return Clean(path)
		}
	}
	return Detect()
}

// Detect 从当前工作目录和可执行文件目录推断应用根目录。
func Detect() string {
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}
	return From(wd, ExeDir())
}

// From 根据工作目录和可执行文件目录识别应用根目录。
//
// 参数说明：
//   - wd：当前工作目录，开发和测试场景优先从这里向上查找 go.work/.git/go.mod。
//   - exeDir：编译后二进制所在目录，生产只部署二进制时作为最终根目录信号。
//
// 设计说明：开发环境通常从子目录执行测试或命令，因此 wd 的 marker 优先；生产环境可能只有
// 根目录下的二进制文件，没有 config/storage/public，wd 无法识别时回退到 exeDir。
func From(wd, exeDir string) string {
	wd = Clean(wd)
	exeDir = Clean(exeDir)

	if hasLayout(wd) {
		return wd
	}
	if root, ok := findMarker(wd); ok {
		return root
	}
	if hasLayout(exeDir) {
		return exeDir
	}
	if hasOwnMarker(exeDir) {
		return exeDir
	}
	if exeDir != "" {
		return exeDir
	}
	return wd
}

// ExeDir 返回当前进程二进制所在目录。
func ExeDir() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	return filepath.Dir(path)
}

// Clean 清理路径，并在可能时转换为绝对路径。
func Clean(path string) string {
	if path == "" {
		path = "."
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

// Join 在 root 下拼接路径；如果传入的是绝对子路径，则保留该绝对路径语义。
func Join(root string, path ...string) string {
	if len(path) > 0 && filepath.IsAbs(path[0]) {
		return filepath.Clean(filepath.Join(path...))
	}
	segments := append([]string{root}, path...)
	return filepath.Clean(filepath.Join(segments...))
}

func hasLayout(dir string) bool {
	for _, name := range []string{"storage", "public", "config", "app", "bootstrap"} {
		if isDir(filepath.Join(dir, name)) {
			return true
		}
	}
	return false
}

func findMarker(start string) (string, bool) {
	dir := start
	tempDir := Clean(os.TempDir())
	for {
		if dir == tempDir && dir != start {
			return "", false
		}
		if hasOwnMarker(dir) {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func hasOwnMarker(dir string) bool {
	for _, marker := range []string{"go.work", ".git", "go.mod"} {
		if _, err := os.Stat(filepath.Join(dir, marker)); err == nil {
			return true
		}
	}
	return false
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
