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

func init() {
	registry.RegisterSink("vector", filament.MaturityAlpha, func() filament.Sink { return sink.New() })
}
