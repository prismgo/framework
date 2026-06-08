package cmd

import (
	makecommand "github.com/prismgo/framework/cmd/make"
	"github.com/prismgo/framework/console"
)

// MakeCommandFactories returns the built-in Prismgo generator command factories.
func MakeCommandFactories() []console.CommandFactory {
	return makecommand.CommandFactories()
}
