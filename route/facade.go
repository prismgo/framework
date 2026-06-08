package route

import (
	"github.com/gin-gonic/gin"
	"github.com/prismgo/framework/facade"
)

const serviceKey = "route.router"

func Resolve() *Router {
	return facade.Resolve[*Router](serviceKey)
}

func Bind(param string, binder Binder)  { Resolve().Bind(param, binder) }
func Model(param string, binder Binder) { Resolve().Model(param, binder) }
func Pattern(param, expr string)        { Resolve().Pattern(param, expr) }
func Mount(engine *gin.Engine) error    { return Resolve().Mount(engine) }
func List() []RouteInfo                 { return Resolve().List() }
func URL(name string, params map[string]any) (string, error) {
	return Resolve().URL(name, params)
}
func Get(uri string, handlers ...HandlerFunc) *Route  { return Resolve().Get(uri, handlers...) }
func Post(uri string, handlers ...HandlerFunc) *Route { return Resolve().Post(uri, handlers...) }
func Put(uri string, handlers ...HandlerFunc) *Route  { return Resolve().Put(uri, handlers...) }
func Patch(uri string, handlers ...HandlerFunc) *Route {
	return Resolve().Patch(uri, handlers...)
}
func Delete(uri string, handlers ...HandlerFunc) *Route {
	return Resolve().Delete(uri, handlers...)
}
func Options(uri string, handlers ...HandlerFunc) *Route {
	return Resolve().Options(uri, handlers...)
}
func Match(methods []string, uri string, handlers ...HandlerFunc) *Route {
	return Resolve().Match(methods, uri, handlers...)
}
func Any(uri string, handlers ...HandlerFunc) *Route { return Resolve().Any(uri, handlers...) }
func Redirect(uri, destination string, status ...int) *Route {
	return Resolve().Redirect(uri, destination, status...)
}
func PermanentRedirect(uri, destination string) *Route {
	return Resolve().PermanentRedirect(uri, destination)
}
func Static(uri, root string) *Route                { return Resolve().Static(uri, root) }
func Fallback(handler HandlerFunc) *Route           { return Resolve().Fallback(handler) }
func Prefix(prefix string) *Registrar               { return Resolve().Prefix(prefix) }
func Name(name string) *Registrar                   { return Resolve().Name(name) }
func Domain(domain string) *Registrar               { return Resolve().Domain(domain) }
func Middleware(handlers ...HandlerFunc) *Registrar { return Resolve().Middleware(handlers...) }
func WithoutMiddleware(names ...string) *Registrar {
	return (&Registrar{router: Resolve()}).WithoutMiddleware(names...)
}
func Controller(controller any) *Registrar { return Resolve().Controller(controller) }
func Group(fn func())                      { Resolve().Group(fn) }
