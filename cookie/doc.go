// Package cookie provides Laravel-style HTTP cookie primitives.
//
// HTTP handlers should use the Request Cookie Queue installed by QueuedCookies
// or http/middleware.StartSession through QueueCookieFrom, QueueMakeFrom, and related
// helpers. The package-level Process Cookie Queue facade remains available for
// non-HTTP scripts, low-level tests, or explicit manual Flush calls.
package cookie
