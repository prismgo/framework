package translation

import (
	"errors"
	"path/filepath"

	"github.com/prismgo/framework/config"
	containercontract "github.com/prismgo/framework/contracts/container"
	providercontract "github.com/prismgo/framework/contracts/provider"
	transcontract "github.com/prismgo/framework/contracts/translation"
)

type providerApplication = providercontract.Application

type ServiceProvider struct{}

func (ServiceProvider) Name() string {
	return "translation"
}

func (ServiceProvider) Provides() []string {
	return []string{
		"translation.loader",
		serviceKey,
	}
}

func (p ServiceProvider) Register(app providerApplication) error {
	c := app.Container()
	if c.Bound(serviceKey) {
		return nil
	}

	basePath := ""
	if raw, err := c.Make("path.base"); err == nil {
		if value, ok := raw.(string); ok {
			basePath = value
		}
	}
	langPath := ""
	if basePath != "" {
		langPath = filepath.Join(basePath, "lang")
	}

	if err := c.Singleton("translation.loader", func(containercontract.Resolver) (any, error) {
		loader := NewFileLoader()
		if langPath != "" {
			loader.AddPath(langPath)
			loader.AddJSONPath(langPath)
		}
		return loader, nil
	}); err != nil {
		return err
	}

	return c.Singleton(serviceKey, func(resolver containercontract.Resolver) (any, error) {
		locale := config.GetString("app.locale", "en")
		fallback := config.GetString("app.fallback_locale", "en")

		raw, err := resolver.Make("translation.loader")
		if err != nil {
			return nil, err
		}

		loader, ok := raw.(transcontract.Loader)
		if !ok || loader == nil {
			return nil, errors.New("translation: resolved loader is not valid")
		}

		translator := NewTranslator(loader, locale, fallback)

		return translator, nil
	})
}

func (ServiceProvider) Boot(providerApplication) error {
	return nil
}
