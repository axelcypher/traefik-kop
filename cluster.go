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
// Redis held something else. An unchanged configuration only renews its TTL.
func (r *peerRegistry) publish(name string, conf dynamic.Configuration) error {
	data, err := json.Marshal(conf)
	if err != nil {
		return fmt.Errorf("failed to encode configuration of %s: %w", name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last[name] = data
	return r.ensure(context.Background(), name, data)
}

// KeepConfAlive renews the configuration of this instance, or restores it.
func (r *peerRegistry) KeepConfAlive() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.last[r.self]
	if !ok {
		return nil
	}
	return r.ensure(context.Background(), r.self, data)
}

// ensure stores data as the configuration of name unless Redis already holds
// it, in which case it renews the TTL. Comparing with Redis rather than with
// the last write also repairs values that a failover or another instance
// replaced. It requires mu to be held by the caller.
func (r *peerRegistry) ensure(ctx context.Context, name string, data []byte) error {
	key := peerKeyPrefix + name
	stored, err := r.client.Get(ctx, key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to read shared configuration of %s: %w", name, err)
	}
	if bytes.Equal(stored, data) {
		if r.ttl <= 0 {
			return nil
		}
		renewed, err := r.client.PExpire(ctx, key, r.ttl).Result()
		if err != nil || renewed {
			return err
		}
	}

	if err := r.client.Set(ctx, key, data, r.ttl).Err(); err != nil {
		return fmt.Errorf("failed to share configuration of %s: %w", name, err)
	}
	log.Info().Str("peer", name).Msg("shared configuration with peers")
	if err := r.client.Publish(ctx, peerChannel, name).Err(); err != nil {
		return fmt.Errorf("failed to notify peers about %s: %w", name, err)
	}
	return nil
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
//   - Routes that also exist locally are left out, so the local Traefik serves
//     them itself: HTTP routes with the same rule, TCP routes with the same rule
//     and entrypoints, UDP routes with the same entrypoints. For HTTP, this also
//     prevents forwarding loops between hosts that publish the same rule.
//   - HTTP routes with the same rule and entrypoints on several peers become one
//     router that balances over all of them. For TCP and UDP, the first peer by
//     name wins.
//   - Services, middlewares and serversTransports that share a name across peers
//     but differ get the suffix _<peer>, and references follow.
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

	localHTTP, localTCP, localUDP := map[string]bool{}, map[string]bool{}, map[string]bool{}
	if own.HTTP != nil {
		for _, router := range own.HTTP.Routers {
			localHTTP[router.Rule] = true
		}
	}
	if own.TCP != nil {
		for _, router := range own.TCP.Routers {
			localTCP[routeGroup(router.Rule, router.EntryPoints)] = true
		}
	}
	if own.UDP != nil {
		for _, router := range own.UDP.Routers {
			localUDP[routeGroup("", router.EntryPoints)] = true
		}
	}

	type route struct {
		peer   string
		name   string
		router *dynamic.Router // as published by the peer
		copied *dynamic.Router // with references to the view's names
	}
	var groups []string
	httpRoutes := make(map[string][]route)
	tcpSeen, udpSeen := map[string]bool{}, map[string]bool{}
	renames := make(map[string]*peerNames, len(peers))

	for _, peer := range sortedKeys(peers) {
		conf := peers[peer]
		names := addPeerObjects(&view, peer, conf)
		renames[peer] = names

		if http := conf.HTTP; http != nil {
			for _, name := range sortedKeys(http.Routers) {
				router := http.Routers[name]
				if localHTTP[router.Rule] {
					log.Debug().Str("peer", peer).Str("router", name).Msg("view: local route takes precedence")
					continue
				}
				key := routeGroup(router.Rule, router.EntryPoints)
				routes, seen := httpRoutes[key]
				if !seen {
					groups = append(groups, key)
				}
				if len(routes) > 0 && routes[len(routes)-1].peer == peer {
					continue // one route per peer and group
				}
				copied := router.DeepCopy()
				copied.Service = renameRef(copied.Service, names.services)
				copied.Middlewares = renameRefs(copied.Middlewares, names.middlewares)
				httpRoutes[key] = append(routes, route{peer, name, router, copied})
			}
		}

		if tcp := conf.TCP; tcp != nil {
			for _, name := range sortedKeys(tcp.Routers) {
				router := tcp.Routers[name]
				key := routeGroup(router.Rule, router.EntryPoints)
				if localTCP[key] || tcpSeen[key] {
					continue
				}
				tcpSeen[key] = true
				copied := router.DeepCopy()
				copied.Service = renameRef(copied.Service, names.tcpServices)
				copied.Middlewares = renameRefs(copied.Middlewares, names.tcpMiddlewares)
				view.TCP.Routers[uniqueName(view.TCP.Routers, stripDocker(name), peer)] = copied
			}
		}

		if udp := conf.UDP; udp != nil {
			for _, name := range sortedKeys(udp.Routers) {
				router := udp.Routers[name]
				key := routeGroup("", router.EntryPoints)
				if localUDP[key] || udpSeen[key] {
					continue
				}
				udpSeen[key] = true
				copied := router.DeepCopy()
				copied.Service = renameRef(copied.Service, names.udpServices)
				view.UDP.Routers[uniqueName(view.UDP.Routers, stripDocker(name), peer)] = copied
			}
		}
	}

	for _, key := range groups {
		group := httpRoutes[key]
		primary := group[0]
		router := primary.copied
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
				lb := balanced.LoadBalancer
				lb.ServersTransport = renameRef(lb.ServersTransport, renames[primary.peer].transports)
				router.Service = addRenamed(view.HTTP.Services, "lb_"+stripDocker(primary.name), primary.peer, balanced)
			} else {
				log.Debug().Str("rule", router.Rule).Str("peer", primary.peer).Msg("view: cannot balance over peers with the same rule; using the first peer")
			}
		}
		view.HTTP.Routers[uniqueName(view.HTTP.Routers, stripDocker(primary.name), primary.peer)] = router
	}

	return view
}

// peerNames maps the names of a peer's objects to their names in the view.
type peerNames struct {
	services, middlewares, transports          map[string]string
	tcpServices, tcpMiddlewares, tcpTransports map[string]string
	udpServices                                map[string]string
}

// addPeerObjects adds the services, middlewares and serversTransports of a
// peer to the view and returns the names they got.
func addPeerObjects(view *dynamic.Configuration, peer string, conf dynamic.Configuration) *peerNames {
	n := &peerNames{
		services: map[string]string{}, middlewares: map[string]string{}, transports: map[string]string{},
		tcpServices: map[string]string{}, tcpMiddlewares: map[string]string{}, tcpTransports: map[string]string{},
		udpServices: map[string]string{},
	}
	if http := conf.HTTP; http != nil {
		for _, name := range sortedKeys(http.ServersTransports) {
			n.transports[name] = addRenamed(view.HTTP.ServersTransports, name, peer, http.ServersTransports[name].DeepCopy())
		}
		for _, name := range sortedKeys(http.Services) {
			svc := http.Services[name].DeepCopy()
			if svc.LoadBalancer != nil {
				svc.LoadBalancer.ServersTransport = renameRef(svc.LoadBalancer.ServersTransport, n.transports)
			}
			n.services[stripDocker(name)] = addRenamed(view.HTTP.Services, stripDocker(name), peer, svc)
		}
		for _, name := range sortedKeys(http.Middlewares) {
			n.middlewares[stripDocker(name)] = addRenamed(view.HTTP.Middlewares, stripDocker(name), peer, http.Middlewares[name].DeepCopy())
		}
	}
	if tcp := conf.TCP; tcp != nil {
		for _, name := range sortedKeys(tcp.ServersTransports) {
			n.tcpTransports[name] = addRenamed(view.TCP.ServersTransports, name, peer, tcp.ServersTransports[name].DeepCopy())
		}
		for _, name := range sortedKeys(tcp.Services) {
			svc := tcp.Services[name].DeepCopy()
			if svc.LoadBalancer != nil {
				svc.LoadBalancer.ServersTransport = renameRef(svc.LoadBalancer.ServersTransport, n.tcpTransports)
			}
			n.tcpServices[stripDocker(name)] = addRenamed(view.TCP.Services, stripDocker(name), peer, svc)
		}
		for _, name := range sortedKeys(tcp.Middlewares) {
			n.tcpMiddlewares[stripDocker(name)] = addRenamed(view.TCP.Middlewares, stripDocker(name), peer, tcp.Middlewares[name].DeepCopy())
		}
	}
	if udp := conf.UDP; udp != nil {
		for _, name := range sortedKeys(udp.Services) {
			n.udpServices[stripDocker(name)] = addRenamed(view.UDP.Services, stripDocker(name), peer, udp.Services[name].DeepCopy())
		}
	}
	return n
}

// addRenamed adds value as name, or as name_peer if the view already holds a
// different value under name. It returns the name used.
func addRenamed[T any](m map[string]*T, name string, peer string, value *T) string {
	if existing, ok := m[name]; ok {
		if reflect.DeepEqual(existing, value) {
			return name
		}
		name += "_" + peer
	}
	m[name] = value
	return name
}

// renameRef maps a reference to an object of the same peer to its view name.
// References to other providers (name@provider) stay as they are.
func renameRef(ref string, names map[string]string) string {
	if renamed, ok := names[stripDocker(ref)]; ok {
		return renamed
	}
	return ref
}

func renameRefs(refs []string, names map[string]string) []string {
	if refs == nil {
		return nil
	}
	renamed := make([]string, len(refs))
	for i, ref := range refs {
		renamed[i] = renameRef(ref, names)
	}
	return renamed
}

// routeGroup identifies routes that compete for the same requests.
func routeGroup(rule string, entryPoints []string) string {
	sorted := append([]string(nil), entryPoints...)
	sort.Strings(sorted)
	return rule + "\x00" + strings.Join(sorted, ",")
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
