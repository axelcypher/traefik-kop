package traefikkop

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

// apiRouter is the part of a router in Traefik's /api/http/routers response
// that the import needs.
type apiRouter struct {
	Name        string                   `json:"name"`
	Provider    string                   `json:"provider"`
	Status      string                   `json:"status"`
	Rule        string                   `json:"rule"`
	RuleSyntax  string                   `json:"ruleSyntax"`
	Priority    int                      `json:"priority"`
	TLS         *dynamic.RouterTLSConfig `json:"tls"`
	Middlewares []string                 `json:"middlewares"`
}

// importSkipsProvider lists router providers that the import leaves out:
// Traefik's own routers, and routes that kop published via Redis, which would
// otherwise travel back to the hosts they came from.
var importSkipsProvider = map[string]bool{"internal": true, "redis": true}

// traefikAPIImporter shares the HTTP routers of another Traefik (e.g. in k3s)
// as a pseudo host, so the views of all kop hosts forward them to it. The
// import never goes to the shared "traefik" root key, which that Traefik may
// read itself.
type traefikAPIImporter struct {
	config   Config
	api      *url.URL
	registry *peerRegistry
	client   *http.Client
}

func newTraefikAPIImporter(config Config, registry *peerRegistry) (*traefikAPIImporter, error) {
	api, err := url.Parse(strings.TrimSuffix(strings.TrimSuffix(config.ImportTraefikAPI, "/"), "/api"))
	if err != nil || (api.Scheme != "http" && api.Scheme != "https") || api.Host == "" {
		return nil, fmt.Errorf("invalid Traefik API URL %q", config.ImportTraefikAPI)
	}
	return &traefikAPIImporter{
		config:   config,
		api:      api,
		registry: registry,
		client:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (im *traefikAPIImporter) run(ctx context.Context) {
	interval := time.Duration(im.config.ImportInterval) * time.Second
	if interval <= 0 {
		interval = 15 * time.Second
	}
	log.Info().Str("api", im.api.Redacted()).Str("peer", im.config.ImportName).Msgf("importing Traefik routers every %s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := im.importOnce(ctx); err != nil {
			// Keep the last import; it expires with the Redis TTL.
			log.Warn().Err(err).Str("peer", im.config.ImportName).Msg("failed to import Traefik routers")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (im *traefikAPIImporter) importOnce(ctx context.Context) error {
	routers, err := im.fetch(ctx)
	if err != nil {
		return err
	}
	return im.registry.publish(im.config.ImportName, buildImportedConfig(routers, im.config))
}

// fetch returns all HTTP routers, following Traefik's pagination.
func (im *traefikAPIImporter) fetch(ctx context.Context) ([]apiRouter, error) {
	var all []apiRouter
	for page := 1; ; {
		u := *im.api
		u.Path = strings.TrimSuffix(u.Path, "/") + "/api/http/routers"
		u.RawQuery = url.Values{"page": {strconv.Itoa(page)}, "per_page": {"100"}}.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := im.client.Do(req)
		if err != nil {
			return nil, err
		}
		var routers []apiRouter
		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("GET %s: %s", u.Redacted(), resp.Status)
		} else {
			err = json.NewDecoder(resp.Body).Decode(&routers)
		}
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		all = append(all, routers...)

		// Traefik sets X-Next-Page to 1 on the last page.
		next, err := strconv.Atoi(resp.Header.Get("X-Next-Page"))
		if err != nil || next <= page {
			return all, nil
		}
		page = next
	}
}

// buildImportedConfig forwards each imported rule to the importing Traefik
// with the Host header intact. That Traefik applies its own middlewares.
func buildImportedConfig(routers []apiRouter, config Config) dynamic.Configuration {
	name := config.ImportName
	source := &dynamic.HTTPConfiguration{Routers: make(map[string]*dynamic.Router)}
	for _, r := range routers {
		if importSkipsProvider[r.Provider] || r.Status == "disabled" || r.Rule == "" {
			continue
		}
		source.Routers[strings.ReplaceAll(r.Name, "@", "-")] = &dynamic.Router{
			Rule:        r.Rule,
			RuleSyntax:  r.RuleSyntax,
			Priority:    r.Priority,
			TLS:         r.TLS,
			Middlewares: r.Middlewares,
		}
	}

	http := &dynamic.HTTPConfiguration{
		Routers:           make(map[string]*dynamic.Router),
		Services:          make(map[string]*dynamic.Service),
		ServersTransports: make(map[string]*dynamic.ServersTransport),
	}
	if len(source.Routers) > 0 {
		transport := addIngressTransport(http, name, config)
		http.Services[name] = ingressService(config.ImportBackend, transport)
		for _, routerName := range selectIngressRouters(source) {
			http.Routers[name+"_"+routerName] = ingressRouter(source.Routers[routerName], name, config)
		}
	}
	return dynamic.Configuration{
		HTTP: http,
		TCP:  &dynamic.TCPConfiguration{},
		UDP:  &dynamic.UDPConfiguration{},
	}
}
