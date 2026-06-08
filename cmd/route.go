package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/prismgo/framework/console"
	"github.com/prismgo/framework/route"
)

// RouteListCommand 展示当前应用已声明的 HTTP 路由。
type RouteListCommand struct {
	loadRoutes func() error
}

func NewRouteListCommand(loadRoutes func() error) *RouteListCommand {
	return &RouteListCommand{loadRoutes: loadRoutes}
}

func (c *RouteListCommand) Definition() *console.Definition {
	return console.MustDefinition("route:list {--json : Output the route list as JSON} {--method= : Filter by HTTP method} {--action= : Filter by action} {--name= : Filter by route name} {--domain= : Filter by domain} {--middleware= : Filter by middleware} {--path= : Filter by URI path} {--except-path= : Exclude URI path} {--r|reverse : Reverse route order} {--sort=uri : Sort by domain, method, uri, name, action, middleware, definition} {--except-vendor : Hide vendor routes} {--only-vendor : Only show vendor routes}", "List registered HTTP routes")
}

func (c *RouteListCommand) Handle(ctx console.CommandContext) error {
	if err := c.ensureRoutesLoaded(); err != nil {
		return err
	}
	routes := filterRouteList(route.List(), routeListFilters{
		method:       strings.TrimSpace(ctx.Input().Option("method")),
		action:       strings.TrimSpace(ctx.Input().Option("action")),
		name:         strings.TrimSpace(ctx.Input().Option("name")),
		domain:       strings.TrimSpace(ctx.Input().Option("domain")),
		middleware:   strings.TrimSpace(ctx.Input().Option("middleware")),
		path:         strings.TrimSpace(ctx.Input().Option("path")),
		exceptPath:   strings.TrimSpace(ctx.Input().Option("except-path")),
		exceptVendor: ctx.Input().OptionBool("except-vendor"),
		onlyVendor:   ctx.Input().OptionBool("only-vendor"),
	})
	sortRouteList(routes, strings.TrimSpace(ctx.Input().Option("sort")), ctx.Input().OptionBool("reverse"))
	if ctx.Input().OptionBool("json") {
		return writeRouteListJSON(console.OutputWriter(ctx.IO()), routes)
	}
	out := console.OutputWriter(ctx.IO())
	opts := console.OutputOptionsForIO(ctx.IO())
	return writeRouteListText(out, routes, opts)
}

type routeListFilters struct {
	method       string
	action       string
	name         string
	domain       string
	middleware   string
	path         string
	exceptPath   string
	exceptVendor bool
	onlyVendor   bool
}

func filterRouteList(routes []route.RouteInfo, filters routeListFilters) []route.RouteInfo {
	out := make([]route.RouteInfo, 0, len(routes))
	methodFilter := strings.ToUpper(filters.method)
	for _, info := range routes {
		if methodFilter != "" && !containsMethod(info.Methods, methodFilter) {
			continue
		}
		if filters.action != "" && !containsFold(info.Handler, filters.action) {
			continue
		}
		if filters.name != "" && !containsFold(info.Name, filters.name) {
			continue
		}
		if filters.domain != "" && !containsFold(info.Domain, filters.domain) {
			continue
		}
		if filters.middleware != "" && !containsFold(strings.Join(info.Middleware, ","), filters.middleware) {
			continue
		}
		if filters.path != "" && !containsFold(info.URI, filters.path) && !containsFold(info.GinPath, filters.path) {
			continue
		}
		if filters.exceptPath != "" && (containsFold(info.URI, filters.exceptPath) || containsFold(info.GinPath, filters.exceptPath)) {
			continue
		}
		vendor := isVendorRoute(info)
		if filters.exceptVendor && vendor {
			continue
		}
		if filters.onlyVendor && !vendor {
			continue
		}
		out = append(out, info)
	}
	return out
}

func sortRouteList(routes []route.RouteInfo, key string, reverse bool) {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		key = "uri"
	}
	sort.SliceStable(routes, func(i, j int) bool {
		left, right := routeSortValue(routes[i], key), routeSortValue(routes[j], key)
		if left == right {
			left, right = routeSortValue(routes[i], "definition"), routeSortValue(routes[j], "definition")
		}
		if reverse {
			return left > right
		}
		return left < right
	})
}

func routeSortValue(info route.RouteInfo, key string) string {
	switch key {
	case "domain":
		return info.Domain
	case "method":
		return strings.Join(info.Methods, ",")
	case "name":
		return info.Name
	case "action":
		return info.Handler
	case "middleware":
		return strings.Join(info.Middleware, ",")
	case "definition":
		return info.Domain + "\x00" + strings.Join(info.Methods, ",") + "\x00" + info.URI + "\x00" + info.Name + "\x00" + info.Handler
	default:
		return info.URI
	}
}

func writeRouteListJSON(out io.Writer, routes []route.RouteInfo) error {
	type routeListJSONRow struct {
		Domain     string   `json:"domain,omitempty"`
		Method     string   `json:"method"`
		URI        string   `json:"uri"`
		Name       string   `json:"name,omitempty"`
		Action     string   `json:"action,omitempty"`
		Middleware []string `json:"middleware,omitempty"`
	}
	rows := make([]routeListJSONRow, 0, len(routes))
	for _, info := range routes {
		rows = append(rows, routeListJSONRow{
			Domain:     info.Domain,
			Method:     strings.Join(info.Methods, "|"),
			URI:        displayRouteURI(info),
			Name:       info.Name,
			Action:     info.Handler,
			Middleware: append([]string(nil), info.Middleware...),
		})
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(rows)
}

func writeRouteListText(out io.Writer, routes []route.RouteInfo, opts console.OutputOptions) error {
	if opts.Quiet || opts.Silent {
		return nil
	}
	fmt.Fprintln(out)
	width := terminalWidth()
	for _, info := range routes {
		line := routeListLine(info, width, opts)
		fmt.Fprintln(out, line)
	}
	fmt.Fprintln(out)
	count := fmt.Sprintf("Showing [%d] routes", len(routes))
	fmt.Fprintln(out, rightAlign(console.Styled(count, console.StyleBlueBold, opts), count, width))
	return nil
}

func routeListLine(info route.RouteInfo, width int, opts console.OutputOptions) string {
	method := strings.Join(info.Methods, "|")
	uri := colorRouteURI(displayRouteURI(info), opts)
	action := info.Handler
	if info.Name != "" {
		action = info.Name + " › " + action
	}
	prefixPlain := fmt.Sprintf("  %-10s %s ", method, displayRouteURI(info))
	prefix := fmt.Sprintf("  %-10s %s ", colorMethod(method, opts), uri)
	dotCount := width - len(prefixPlain) - len(action) - 3
	if dotCount < 3 {
		dotCount = 3
	}
	return prefix + console.Styled(strings.Repeat(".", dotCount), console.StyleMuted, opts) + " " + action
}

func colorMethod(method string, opts console.OutputOptions) string {
	style := console.StyleInfo
	switch {
	case strings.Contains(method, "POST"):
		style = console.StyleYellow
	case strings.Contains(method, "PUT"), strings.Contains(method, "PATCH"):
		style = console.StyleBlueBold
	case strings.Contains(method, "DELETE"):
		style = console.StyleError
	}
	return console.Styled(method, style, opts)
}

func colorRouteURI(uri string, opts console.OutputOptions) string {
	if !opts.ANSI {
		return uri
	}
	var out strings.Builder
	for len(uri) > 0 {
		start := strings.Index(uri, "{")
		if start == -1 {
			out.WriteString(console.Styled(uri, console.StyleWhiteBold, opts))
			break
		}
		if start > 0 {
			out.WriteString(console.Styled(uri[:start], console.StyleWhiteBold, opts))
		}
		end := strings.Index(uri[start:], "}")
		if end == -1 {
			out.WriteString(console.Styled(uri[start:], console.StyleWhiteBold, opts))
			break
		}
		end += start
		out.WriteString(console.Styled(uri[start:end+1], console.StyleYellow, opts))
		uri = uri[end+1:]
	}
	return out.String()
}

func displayRouteURI(info route.RouteInfo) string {
	if info.URI != "" {
		return info.URI
	}
	return info.GinPath
}

func terminalWidth() int {
	if columns := strings.TrimSpace(os.Getenv("COLUMNS")); columns != "" {
		var value int
		if _, err := fmt.Sscanf(columns, "%d", &value); err == nil && value > 20 {
			return value
		}
	}
	return 80
}

func rightAlign(styled, plain string, width int) string {
	padding := width - len(plain)
	if padding < 0 {
		padding = 0
	}
	return strings.Repeat(" ", padding) + styled
}

func containsFold(value, needle string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(needle))
}

func isVendorRoute(info route.RouteInfo) bool {
	path := strings.ReplaceAll(info.SourcePath, "\\", "/")
	return strings.Contains(path, "/vendor/") || strings.Contains(path, "/pkg/mod/")
}

func containsMethod(methods []string, want string) bool {
	for _, method := range methods {
		if method == want {
			return true
		}
	}
	return false
}

func (c *RouteListCommand) ensureRoutesLoaded() error {
	if c.loadRoutes != nil {
		return c.loadRoutes()
	}
	if len(route.List()) > 0 {
		return nil
	}
	return fmt.Errorf("route:list: route loader is not configured")
}
