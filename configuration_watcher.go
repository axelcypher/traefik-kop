package traefikkop

import (
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/provider"
	"github.com/traefik/traefik/v3/pkg/safe"
	"github.com/traefik/traefik/v3/pkg/server"
	"sync"
)

// configurationRefresh retains the original provider configuration, before kop
// rewrites service names and endpoints. Refresh also evaluates kop-only labels,
// which Traefik excludes from its configuration change detection.
type configurationRefresh struct {
	mu     sync.Mutex
	latest *dynamic.Configuration
	handle func(dynamic.Configuration)
}

func (r *configurationRefresh) Update(conf dynamic.Configuration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latest = conf.DeepCopy()
	r.handle(*r.latest.DeepCopy())
}

func (r *configurationRefresh) Refresh() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.latest != nil {
		r.handle(*r.latest.DeepCopy())
	}
}

// newConfigurationWatcher leaves default entrypoint selection to the remote
// Traefik instance. Since 3.7, the watcher requires an entrypoint to retain TLS
// routers, so supply a temporary default and remove it before publishing.
func newConfigurationWatcher(pool *safe.Pool, p provider.Provider) *server.ConfigurationWatcher {
	const defaultEntryPoint = "traefik-kop-default-internal"
	watcher := server.NewConfigurationWatcher(pool, p, []string{defaultEntryPoint}, "docker", false)
	watcher.AddListener(func(conf dynamic.Configuration) {
		if conf.HTTP != nil {
			for _, router := range conf.HTTP.Routers {
				if len(router.EntryPoints) == 1 && router.EntryPoints[0] == defaultEntryPoint {
					router.EntryPoints = nil
				}
			}
		}
		if conf.TCP != nil {
			for _, router := range conf.TCP.Routers {
				if len(router.EntryPoints) == 1 && router.EntryPoints[0] == defaultEntryPoint {
					router.EntryPoints = nil
				}
			}
		}
	})
	return watcher
}
