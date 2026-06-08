// Package container provides PrismGo's application service container.
//
// The container package is the concrete implementation behind
// foundation.Application.Container. Framework packages bind services by stable
// Laravel-style string keys, then package-level facade helpers resolve those
// services from the current application container.
//
// Provider registration should bind factories instead of creating heavy
// resources immediately:
//
//	app.Container().Singleton("cache.manager", func(r containercontract.Resolver) (any, error) {
//		return cache.NewManager(config), nil
//	})
//
// Application code that has an explicit resolver should prefer typed Make:
//
//	manager, err := container.Make[*cache.Manager](app.Container(), "cache.manager")
//
// Package-level helpers such as Resolve and Use exist for facade-style APIs
// that do not receive an application instance. Tests and provider code should
// prefer explicit container arguments where possible, because the current
// container pointer is process-wide.
package container
