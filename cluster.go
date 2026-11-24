package traefikkop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/safe"
)

const (
	// peerKeyPrefix holds the published configuration of each kop instance
	// (and of imported pseudo hosts) as JSON, e.g. kop/hosts/docker-01.
	peerKeyPrefix = "kop/hosts/"
	// peerChannel announces the name of a peer whose configuration changed.
	peerChannel = "kop:updates"
	// viewResyncInterval bounds how long a view keeps routes of a peer whose
	// configuration expired, or misses an update notification.
	viewResyncInterval = 10 * time.Second
)

// viewRootKey is the Redis root key of the view for the given host. Point the
// Redis provider of that host's Traefik at it.
func viewRootKey(hostname string) string {
	return defaultRootKey + "-" + hostname
}

type keepAliver interface {
	KeepConfAlive() error
}

// keepAliveGroup renews several published configurations.
type keepAliveGroup []keepAliver

func (g keepAliveGroup) KeepConfAlive() error {
	var errs []error
	for _, k := range g {
		if err := k.KeepConfAlive(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// peerRegistry shares published configurations between kop instances.
type peerRegistry struct {
	client *redis.Client
	ttl    time.Duration
	self   string

	mu   sync.Mutex
	last map[string][]byte
}

func newPeerRegistry(client *redis.Client, self string, ttl int) *peerRegistry {
	return &peerRegistry{
		client: client,
		ttl:    time.Duration(ttl) * time.Second,
		self:   self,
		last:   make(map[string][]byte),
	}
}

// publish shares conf as the configuration of name and notifies the peers if
// it changed. An unchanged configuration only renews its TTL.
func (r *peerRegistry) publish(name string, conf dynamic.Configuration) error {
	data, err := json.Marshal(conf)
	if err != nil {
		return fmt.Errorf("failed to encode configuration of %s: %w", name, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	ctx := context.Background()
	if bytes.Equal(r.last[name], data) {
		stored, err := r.renew(ctx, name)
		if err != nil || stored {
			return err
		}
	}
	return r.write(ctx, name, data)
}

// KeepConfAlive renews the configuration of this instance, or restores it.
func (r *peerRegistry) KeepConfAlive() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.last[r.self]
	if !ok {
		return nil
	}
	ctx := context.Background()
	stored, err := r.renew(ctx, r.self)
	if err != nil || stored {
		return err
	}
	log.Warn().Str("peer", r.self).Msg("Redis lost shared configuration. Pushing last known configuration")
	return r.write(ctx, r.self, data)
}

// write requires mu to be held by the caller.
func (r *peerRegistry) write(ctx context.Context, name string, data []byte) error {
	if err := r.client.Set(ctx, peerKeyPrefix+name, data, r.ttl).Err(); err != nil {
		return fmt.Errorf("failed to share configuration of %s: %w", name, err)
	}
	r.last[name] = data
	log.Info().Str("peer", name).Msg("shared configuration with peers")
	if err := r.client.Publish(ctx, peerChannel, name).Err(); err != nil {
		return fmt.Errorf("failed to notify peers about %s: %w", name, err)
	}
	return nil
}

// renew reports whether the configuration of name is still stored.
func (r *peerRegistry) renew(ctx context.Context, name string) (bool, error) {
	key := peerKeyPrefix + name
	if r.ttl > 0 {
		return r.client.PExpire(ctx, key, r.ttl).Result()
	}
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

// load returns all shared configurations by peer name.
func (r *peerRegistry) load(ctx context.Context) (map[string]dynamic.Configuration, error) {
	var keys []string
	iter := r.client.Scan(ctx, 0, peerKeyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to list peers: %w", err)
	}

	peers := make(map[string]dynamic.Configuration, len(keys))
	if len(keys) == 0 {
		return peers, nil
	}
	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read peers: %w", err)
	}
	for i, value := range values {
		data, ok := value.(string)
		if !ok {
			continue // expired after listing
		}
		name := strings.TrimPrefix(keys[i], peerKeyPrefix)
		var conf dynamic.Configuration
		if err := json.Unmarshal([]byte(data), &conf); err != nil {
			log.Warn().Err(err).Str("peer", name).Msg("ignoring unreadable peer configuration")
			continue
		}
		peers[name] = conf
	}
	return peers, nil
}

// viewBuilder publishes the routes of all other hosts for the local Traefik.
type viewBuilder struct {
	self     string
	registry *peerRegistry
	store    *RedisStore
	trigger  chan struct{}

	mu  sync.Mutex
	own *dynamic.Configuration
}

func newViewBuilder(client *redis.Client, registry *peerRegistry, self string, ttl int) *viewBuilder {
	store := newRedisStore(client, self, viewRootKey(self), ttl)
	store.quiet = true
	return &viewBuilder{
		self:     self,
		registry: registry,
		store:    store,
		trigger:  make(chan struct{}, 1),
	}
}

// setOwn records the configuration of this host and schedules a rebuild.
func (v *viewBuilder) setOwn(conf dynamic.Configuration) {
	v.mu.Lock()
	v.own = conf.DeepCopy()
	v.mu.Unlock()
	v.schedule()
}

// schedule requests a rebuild. Pending requests coalesce.
func (v *viewBuilder) schedule() {
	select {
	case v.trigger <- struct{}{}:
	default:
	}
}

// run rebuilds the view when scheduled, when a peer announces a change, and
// periodically to drop peers whose configuration expired.
func (v *viewBuilder) run(ctx context.Context) {
	pubsub := v.registry.client.Subscribe(ctx, peerChannel)
	defer pubsub.Close()
	messages := pubsub.Channel()
	ticker := time.NewTicker(viewResyncInterval)
	defer ticker.Stop()

	for {
		select {
		case msg := <-messages:
			if msg != nil && msg.Payload != v.self {
				log.Debug().Str("peer", msg.Payload).Msg("peer configuration changed")
				v.rebuild(ctx)
			}
		case <-v.trigger:
			v.rebuild(ctx)
		case <-ticker.C:
			v.rebuild(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (v *viewBuilder) rebuild(ctx context.Context) {
	if err := v.rebuildErr(ctx); err != nil {
		log.Warn().Err(err).Str("root", v.store.root).Msg("failed to update host view")
	}
}

func (v *viewBuilder) rebuildErr(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.own == nil {
		// Until the local routes are known, peer routes could shadow them.
		return nil
	}
	peers, err := v.registry.load(ctx)
	if err != nil {
		return err
	}
	delete(peers, v.self)
	return v.store.Store(buildView(*v.own, peers))
}

func (v *viewBuilder) KeepConfAlive() error {
	return v.store.KeepConfAlive()
}

// buildView combines the configurations of all peers for a host whose own
// published configuration is own.
//
//   - Routes whose rule (or UDP router name) also exists locally are left out,
//     so the local Traefik serves them itself. This also prevents forwarding
//     loops between hosts that publish the same rule.
//   - HTTP routes with the same rule and entrypoints on several peers become one
//     router that balances over all of them. TCP and UDP keep the first peer by
//     name.
func buildView(own dynamic.Configuration, peers map[string]dynamic.Configuration) dynamic.Configuration {
	view := dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers:           make(map[string]*dynamic.Router),
			Services:          make(map[string]*dynamic.Service),
			Middlewares:       make(map[string]*dynamic.Middleware),
			ServersTransports: make(map[string]*dynamic.ServersTransport),
		},
		TCP: &dynamic.TCPConfiguration{
			Routers:           make(map[string]*dynamic.TCPRouter),
			Services:          make(map[string]*dynamic.TCPService),
			Middlewares:       make(map[string]*dynamic.TCPMiddleware),
			ServersTransports: make(map[string]*dynamic.TCPServersTransport),
		},
		UDP: &dynamic.UDPConfiguration{
			Routers:  make(map[string]*dynamic.UDPRouter),
			Services: make(map[string]*dynamic.UDPService),
		},
	}

	names := make([]string, 0, len(peers))
	for name := range peers {
		names = append(names, name)
	}
	sort.Strings(names)

	localHTTP, localTCP, localUDP := map[string]bool{}, map[string]bool{}, map[string]bool{}
	if own.HTTP != nil {
		for _, router := range own.HTTP.Routers {
			localHTTP[router.Rule] = true
		}
	}
	if own.TCP != nil {
		for _, router := range own.TCP.Routers {
			localTCP[router.Rule] = true
		}
	}
	if own.UDP != nil {
		for name := range own.UDP.Routers {
			localUDP[stripDocker(name)] = true
		}
	}

	type route struct {
		peer   string
		name   string
		router *dynamic.Router
	}
	var groups []string
	httpRoutes := make(map[string][]route)
	tcpRules := make(map[string]bool)

	for _, peer := range names {
		conf := peers[peer]
		if http := conf.HTTP; http != nil {
			for _, name := range sortedKeys(http.Routers) {
				router := http.Routers[name]
				if localHTTP[router.Rule] {
					log.Debug().Str("peer", peer).Str("router", name).Msg("view: local route takes precedence")
					continue
				}
				key := routeGroup(router)
				routes, seen := httpRoutes[key]
				if !seen {
					groups = append(groups, key)
				}
				if len(routes) > 0 && routes[len(routes)-1].peer == peer {
					continue // one route per peer and group
				}
				httpRoutes[key] = append(routes, route{peer, name, router})
			}
			for _, name := range sortedKeys(http.Services) {
				addService(view.HTTP.Services, stripDocker(name), http.Services[name])
			}
			for _, name := range sortedKeys(http.Middlewares) {
				addFirst(view.HTTP.Middlewares, stripDocker(name), http.Middlewares[name])
			}
			for _, name := range sortedKeys(http.ServersTransports) {
				addFirst(view.HTTP.ServersTransports, name, http.ServersTransports[name])
			}
		}

		if tcp := conf.TCP; tcp != nil {
			for _, name := range sortedKeys(tcp.Routers) {
				router := tcp.Routers[name]
				if localTCP[router.Rule] || tcpRules[router.Rule] {
					continue
				}
				tcpRules[router.Rule] = true
				addFirst(view.TCP.Routers, uniqueName(view.TCP.Routers, stripDocker(name), peer), router)
			}
			for _, name := range sortedKeys(tcp.Services) {
				addFirst(view.TCP.Services, stripDocker(name), tcp.Services[name])
			}
			for _, name := range sortedKeys(tcp.Middlewares) {
				addFirst(view.TCP.Middlewares, stripDocker(name), tcp.Middlewares[name])
			}
			for _, name := range sortedKeys(tcp.ServersTransports) {
				addFirst(view.TCP.ServersTransports, name, tcp.ServersTransports[name])
			}
		}

		if udp := conf.UDP; udp != nil {
			for _, name := range sortedKeys(udp.Routers) {
				if localUDP[stripDocker(name)] {
					continue
				}
				addFirst(view.UDP.Routers, stripDocker(name), udp.Routers[name])
			}
			for _, name := range sortedKeys(udp.Services) {
				addFirst(view.UDP.Services, stripDocker(name), udp.Services[name])
			}
		}
	}

	for _, key := range groups {
		group := httpRoutes[key]
		primary := group[0]
		router := primary.router.DeepCopy()
		if len(group) > 1 {
			backends := make([]backend, len(group))
			for i, r := range group {
				http := peers[r.peer].HTTP
				backends[i].service = lookup(http.Services, r.router.Service)
				if backends[i].service != nil && backends[i].service.LoadBalancer != nil {
					backends[i].transport = http.ServersTransports[backends[i].service.LoadBalancer.ServersTransport]
				}
			}
			if balanced := balanceBackends(backends); balanced != nil {
				name := "lb_" + stripDocker(primary.name)
				view.HTTP.Services[name] = balanced
				router.Service = name
			} else {
				log.Debug().Str("rule", router.Rule).Str("peer", primary.peer).Msg("view: cannot balance over peers with the same rule; using the first peer")
			}
		}
		view.HTTP.Routers[uniqueName(view.HTTP.Routers, stripDocker(primary.name), primary.peer)] = router
	}

	return view
}

// routeGroup identifies HTTP routes that compete for the same requests.
func routeGroup(router *dynamic.Router) string {
	entryPoints := append([]string(nil), router.EntryPoints...)
	sort.Strings(entryPoints)
	return router.Rule + "\x00" + strings.Join(entryPoints, ",")
}

type backend struct {
	service   *dynamic.Service
	transport *dynamic.ServersTransport
}

// balanceBackends returns a load balancer over the servers of all backends, or
// nil unless all are load balancers that differ only in their servers and in
// the names of equivalent serversTransports.
func balanceBackends(backends []backend) *dynamic.Service {
	if backends[0].service == nil || backends[0].service.LoadBalancer == nil {
		return nil
	}
	settings := func(lb *dynamic.ServersLoadBalancer) *dynamic.ServersLoadBalancer {
		lb = lb.DeepCopy()
		lb.Servers, lb.ServersTransport = nil, ""
		return lb
	}
	reference := settings(backends[0].service.LoadBalancer)

	balanced := backends[0].service.DeepCopy()
	balanced.LoadBalancer.Servers = nil
	seen := make(map[string]bool)
	for _, b := range backends {
		if b.service == nil || b.service.LoadBalancer == nil ||
			!reflect.DeepEqual(reference, settings(b.service.LoadBalancer)) ||
			!reflect.DeepEqual(backends[0].transport, b.transport) {
			return nil
		}
		for _, server := range b.service.LoadBalancer.Servers {
			if !seen[server.URL] {
				seen[server.URL] = true
				balanced.LoadBalancer.Servers = append(balanced.LoadBalancer.Servers, server)
			}
		}
	}
	return balanced
}

// addService adds svc, combining the servers of load balancers that share a
// name across peers (as published by kop's load balancer merging).
func addService(services map[string]*dynamic.Service, name string, svc *dynamic.Service) {
	existing, ok := services[name]
	if !ok {
		services[name] = svc.DeepCopy()
		return
	}
	if existing.LoadBalancer == nil || svc.LoadBalancer == nil {
		return
	}
	for _, server := range svc.LoadBalancer.Servers {
		known := false
		for _, s := range existing.LoadBalancer.Servers {
			known = known || s.URL == server.URL
		}
		if !known {
			existing.LoadBalancer.Servers = append(existing.LoadBalancer.Servers, server)
		}
	}
}

func addFirst[T any](m map[string]T, name string, value T) {
	if _, ok := m[name]; !ok {
		m[name] = value
	}
}

// uniqueName returns name, or name_peer if a different peer already uses it.
func uniqueName[T any](m map[string]T, name string, peer string) string {
	if _, ok := m[name]; ok {
		return name + "_" + peer
	}
	return name
}

func lookup[T any](m map[string]*T, name string) *T {
	if v, ok := m[name]; ok {
		return v
	}
	if v, ok := m[name+"@docker"]; ok {
		return v
	}
	return m[stripDocker(name)]
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// startPeering shares the configuration of this host with its peers and, if
// enabled, maintains the view of this host and the Traefik API import. It
// returns the hook for processed configurations and what needs renewing.
func startPeering(pool *safe.Pool, config Config, client *redis.Client) (func(dynamic.Configuration), keepAliveGroup) {
	registry := newPeerRegistry(client, config.Hostname, config.RedisTTL)
	keep := keepAliveGroup{registry}

	var view *viewBuilder
	if config.View {
		view = newViewBuilder(client, registry, config.Hostname, config.RedisTTL)
		keep = append(keep, view)
		pool.GoCtx(view.run)
		log.Info().Msgf("publishing the routes of all other hosts to %s/", view.store.root)
	}

	if config.ImportTraefikAPI != "" {
		importer, err := newTraefikAPIImporter(config, registry)
		if err != nil {
			log.Fatal().Err(err).Msg("invalid Traefik API import")
		}
		pool.GoCtx(importer.run)
	}

	return func(conf dynamic.Configuration) {
		if err := registry.publish(config.Hostname, conf); err != nil {
			log.Warn().Err(err).Msg("failed to share configuration with peers")
		}
		if view != nil {
			view.setOwn(conf)
		}
	}, keep
}
