package route

import "github.com/gin-gonic/gin"

// ResourceController 描述 Laravel resource 路由需要的控制器方法集合。
type ResourceController interface {
	Index(*gin.Context)
	Store(*gin.Context)
	Show(*gin.Context)
	Update(*gin.Context)
	Destroy(*gin.Context)
}

// CreateController 是完整 Resource 中可选的 create 页面动作。
type CreateController interface {
	Create(*gin.Context)
}

// EditController 是完整 Resource 中可选的 edit 页面动作。
type EditController interface {
	Edit(*gin.Context)
}

type resourceConfig struct {
	only       map[string]struct{}
	except     map[string]struct{}
	names      map[string]string
	parameters map[string]string
	apiOnly    bool
}

// ResourceOption 配置 Resource/ApiResource 的动作集合、名称和参数名。
type ResourceOption func(*resourceConfig)

func Only(actions ...string) ResourceOption {
	return func(cfg *resourceConfig) { cfg.only = stringSet(actions...) }
}

func Except(actions ...string) ResourceOption {
	return func(cfg *resourceConfig) { cfg.except = stringSet(actions...) }
}

func Names(names map[string]string) ResourceOption {
	return func(cfg *resourceConfig) { cfg.names = names }
}

func Parameters(parameters map[string]string) ResourceOption {
	return func(cfg *resourceConfig) { cfg.parameters = parameters }
}

// Resource 注册完整资源路由；create/edit 只有控制器实现对应接口时才会注册。
func (r *Router) Resource(name string, controller ResourceController, options ...ResourceOption) []*Route {
	cfg := buildResourceConfig(false, options...)
	return r.resource(name, controller, cfg)
}

// ApiResource 注册 API 资源路由，默认排除 create/edit。
func (r *Router) ApiResource(name string, controller ResourceController, options ...ResourceOption) []*Route {
	cfg := buildResourceConfig(true, options...)
	return r.resource(name, controller, cfg)
}

func (r *Router) ApiResources(resources map[string]ResourceController, options ...ResourceOption) []*Route {
	routes := make([]*Route, 0)
	for name, controller := range resources {
		routes = append(routes, r.ApiResource(name, controller, options...)...)
	}
	return routes
}

func Resource(name string, controller ResourceController, options ...ResourceOption) []*Route {
	return Resolve().Resource(name, controller, options...)
}

func ApiResource(name string, controller ResourceController, options ...ResourceOption) []*Route {
	return Resolve().ApiResource(name, controller, options...)
}

func ApiResources(resources map[string]ResourceController, options ...ResourceOption) []*Route {
	return Resolve().ApiResources(resources, options...)
}

func (r *Router) resource(name string, controller ResourceController, cfg resourceConfig) []*Route {
	param := requireResourceParam(name)
	if override := cfg.parameters[name]; override != "" {
		param = override
	}
	base := resourceURI(name)
	member := resourceMemberURI(name, param)
	namePrefix := resourceNamePrefix(name)

	routes := make([]*Route, 0, 7)
	add := func(action string, register func() *Route) {
		if !cfg.allows(action) {
			return
		}
		route := register()
		if routeName := cfg.routeName(namePrefix, action); routeName != "" {
			route.Name(routeName)
		}
		routes = append(routes, route)
	}

	add("index", func() *Route { return r.Get(base, controller.Index) })
	add("store", func() *Route { return r.Post(base, controller.Store) })
	if !cfg.apiOnly {
		if create, ok := controller.(CreateController); ok {
			add("create", func() *Route { return r.Get(base+"/create", create.Create) })
		}
		if edit, ok := controller.(EditController); ok {
			add("edit", func() *Route { return r.Get(member+"/edit", edit.Edit) })
		}
	}
	add("show", func() *Route { return r.Get(member, controller.Show) })
	add("update", func() *Route { return r.Match([]string{"PUT", "PATCH"}, member, controller.Update) })
	add("destroy", func() *Route { return r.Delete(member, controller.Destroy) })
	return routes
}

func buildResourceConfig(apiOnly bool, options ...ResourceOption) resourceConfig {
	cfg := resourceConfig{
		names:      map[string]string{},
		parameters: map[string]string{},
		apiOnly:    apiOnly,
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return cfg
}

func (cfg resourceConfig) allows(action string) bool {
	if _, skip := cfg.except[action]; skip {
		return false
	}
	if len(cfg.only) == 0 {
		return true
	}
	_, ok := cfg.only[action]
	return ok
}

func (cfg resourceConfig) routeName(prefix, action string) string {
	if name := cfg.names[action]; name != "" {
		return name
	}
	if prefix == "" || action == "" {
		return ""
	}
	return prefix + "." + action
}

func stringSet(values ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}
