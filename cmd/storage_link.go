package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	configpkg "github.com/prismgo/framework/config"
	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/support"
)

// StorageLinkCommand creates the configured public storage symbolic links.
type StorageLinkCommand struct{}

// StorageUnlinkCommand removes the configured public storage symbolic links.
type StorageUnlinkCommand struct{}

// NewStorageLinkCommand creates the storage:link command.
func NewStorageLinkCommand() *StorageLinkCommand {
	return &StorageLinkCommand{}
}

// NewStorageUnlinkCommand creates the storage:unlink command.
func NewStorageUnlinkCommand() *StorageUnlinkCommand {
	return &StorageUnlinkCommand{}
}

// Definition returns the storage:link command signature.
func (c *StorageLinkCommand) Definition() *console.Definition {
	return console.MustDefinition(
		"storage:link {--relative : Create the symbolic link using relative paths} {--force : Recreate existing symbolic links}",
		"Create the symbolic links configured for the application",
	)
}

// Handle creates each configured link and returns the first filesystem error.
func (c *StorageLinkCommand) Handle(commandCtx console.CommandContext) error {
	for link, target := range storageLinks() {
		linkTarget := target
		if commandCtx.OptionBool("relative") {
			relativeTarget, err := filepath.Rel(filepath.Dir(link), target)
			if err != nil {
				return err
			}
			linkTarget = relativeTarget
		}

		if err := createStorageLink(link, linkTarget, commandCtx.OptionBool("force")); err != nil {
			return err
		}
	}
	return nil
}

// Definition returns the storage:unlink command signature.
func (c *StorageUnlinkCommand) Definition() *console.Definition {
	return console.MustDefinition("storage:unlink", "Delete existing symbolic links configured for the application")
}

// Handle removes configured link paths and tolerates links that are already absent.
func (c *StorageUnlinkCommand) Handle(console.CommandContext) error {
	for link := range storageLinks() {
		info, err := os.Lstat(link)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			continue
		}
		if err := os.Remove(link); err != nil {
			return err
		}
	}
	return nil
}

func createStorageLink(link string, target string, force bool) error {
	if info, err := os.Lstat(link); err == nil {
		// Force is intentionally limited to symlinks so user files and directories are preserved.
		if !force || info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("storage link %q already exists", link)
		}
		if err := os.Remove(link); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, link)
}

func storageLinks() map[string]string {
	links := make(map[string]string)
	for link, target := range configpkg.GetStringMap("filesystem.links") {
		targetPath, ok := target.(string)
		if !ok {
			continue
		}

		linkPath := strings.TrimSpace(link)
		targetPath = strings.TrimSpace(targetPath)
		if linkPath == "" || targetPath == "" {
			continue
		}
		links[resolveStorageLinkPath(linkPath)] = resolveStorageLinkPath(targetPath)
	}

	if len(links) == 0 {
		links[support.PublicPath("storage")] = support.StoragePath("app/public")
	}
	return links
}

func resolveStorageLinkPath(path string) string {
	path = strings.TrimSpace(path)
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return support.BasePath(path)
}
