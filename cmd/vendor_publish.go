package cmd

import (
	"fmt"
	"strconv"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/provider/publish"
	"github.com/prismgo/framework/support"
)

// vendorPublishCommand 实现 artisan vendor:publish 命令。
type vendorPublishCommand struct{}

// NewVendorPublishCommand 创建 vendor:publish 命令实例。
func NewVendorPublishCommand() console.Command {
	return &vendorPublishCommand{}
}

// Definition 返回命令定义。
func (c *vendorPublishCommand) Definition() *console.Definition {
	d := &console.Definition{
		Name:        "vendor:publish",
		Description: "Publish package assets to the application",
		Help: `The vendor:publish command copies registered publishable assets (config files,
language files, migration files, etc.) from extension packages into the application.

Options:
  --provider   Only publish assets for the specified provider
  --tag        Filter by tag (comma-separated, e.g. config,lang)
  --force      Overwrite existing target files
  --all        Publish all providers and all tags
  --existing   Only update existing target files
  --dry-run    Preview what would be published without copying`,
		Examples: []string{
			"vendor:publish",
			"vendor:publish --provider=acme",
			"vendor:publish --tag=config",
			"vendor:publish --tag=config,lang",
			"vendor:publish --all",
			"vendor:publish --all --dry-run",
			"vendor:publish --tag=migrations --force",
		},
		Hidden: support.IsProduction(),
		Options: []console.Option{
			{
				Name:        "provider",
				Shortcut:    "p",
				Description: "Filter by provider name",
				ValueMode:   console.OptionValueRequired,
			},
			{
				Name:        "tag",
				Shortcut:    "t",
				Description: "Filter by tag (comma-separated)",
				ValueMode:   console.OptionValueRequired,
				IsArray:     true,
			},
			{
				Name:        "force",
				Shortcut:    "f",
				Description: "Overwrite existing files",
				ValueMode:   console.OptionValueNone,
			},
			{
				Name:        "all",
				Shortcut:    "a",
				Description: "Publish all providers and all tags",
				ValueMode:   console.OptionValueNone,
			},
			{
				Name:        "existing",
				Shortcut:    "e",
				Description: "Only update existing target files",
				ValueMode:   console.OptionValueNone,
			},
			{
				Name:        "dry-run",
				Shortcut:    "n",
				Description: "Preview without actually copying",
				ValueMode:   console.OptionValueNone,
			},
		},
	}
	return d
}

// Handle 执行 vendor:publish 命令逻辑。
func (c *vendorPublishCommand) Handle(commandCtx console.CommandContext) error {
	if support.IsProduction() {
		return fmt.Errorf("vendor:publish is not available in production environment")
	}

	provider := commandCtx.Option("provider")
	tags := commandCtx.OptionStrings("tag")
	force := commandCtx.OptionBool("force")
	all := commandCtx.OptionBool("all")
	existing := commandCtx.OptionBool("existing")
	dryRun := commandCtx.OptionBool("dry-run")

	if all {
		provider = ""
		tags = nil
	}

	if dryRun {
		return c.handleDryRun(commandCtx, provider, tags)
	}

	published, skipped, err := publish.Copy(provider, tags, force, existing)
	if err != nil {
		return err
	}

	summary := fmt.Sprintf("vendor:publish — %d file(s) published, %d skipped", published, skipped)
	if published > 0 {
		commandCtx.IO().Success(summary)
	} else {
		commandCtx.IO().Info(summary)
	}

	if published > 0 {
		commandCtx.IO().Info("-------------------------------------------------")
		event.Dispatch(commandCtx.Context(), event.VendorTagPublished{
			Tag:       tags,
			Published: published,
			Skipped:   skipped,
		})
	}

	return nil
}

func (c *vendorPublishCommand) handleDryRun(ctx console.CommandContext, provider string, tags []string) error {
	items, err := publish.DryRun(provider, tags)
	if err != nil {
		return err
	}

	ctx.IO().Info("vendor:publish — preview mode, no files will be copied")

	headers := []string{"Provider", "Source", "Target", "Tag", "Migration", "Exists"}
	var rows [][]string
	for _, item := range items {
		rows = append(rows, []string{
			item.Provider,
			item.Source,
			item.Target,
			item.Tag,
			strconv.FormatBool(item.IsMigration),
			strconv.FormatBool(item.Exists),
		})
	}

	return ctx.IO().Table(headers, rows)
}
