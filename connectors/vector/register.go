// Package vector registers the Vector Store sink with the default registry.
// Enable with:
//
//	import _ "github.com/galaxy-io/filament/connectors/vector"
package vector

import (
	"github.com/galaxy-io/filament"
	"github.com/galaxy-io/filament/connectors/vector/sink"
	"github.com/galaxy-io/filament/registry"
)

// init registers the vector store sink with the default registry on package import.
func init() {
	registry.RegisterSink("vector", filament.MaturityAlpha, func() filament.Sink { return sink.New() })
}
