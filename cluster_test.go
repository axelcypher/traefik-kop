package traefikkop

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/docker/docker/api/types/container"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/provider/docker"
)

func ingressPeer(host, ip string, rules map[string]string) dynamic.Configuration {
	config := Config{Hostname: host, BindIP: ip, DefaultPort: 443}
	routers := make(map[string]*dynamic.Router)
	for name, rule := range rules {
		routers[name+"@docker"] = &dynamic.Router{Rule: rule, Service: name}
	}
	client := createTestClient(map[string]string{})
	dc := &dockerCache{client: client, details: map[string]container.InspectResponse{}}
	return dynamic.Configuration{
		HTTP: buildHTTPIngress(dc, &dynamic.HTTPConfiguration{Routers: routers}, config),
		TCP:  &dynamic.TCPConfiguration{},
		UDP:  &dynamic.UDPConfiguration{},
	}
}

func TestBuildViewPrefersLocalRoutesAndBalancesPeers(t *testing.T) {
	own := ingressPeer("docker-01", "10.8.0.25", map[string]string{"shared": "Host(`shared.example.com`)"})
	peers := map[string]dynamic.Configuration{
		"docker-02": ingressPeer("docker-02", "10.8.0.26", map[string]string{
			"shared": "Host(`shared.example.com`)",
			"ha":     "Host(`ha.example.com`)",
			"two":    "Host(`two.example.com`)",
		}),
		"docker-03": ingressPeer("docker-03", "10.8.0.27", map[string]string{
			"ha": "Host(`ha.example.com`)",
		}),
	}

	view := buildView(own, peers)

	// docker-01 serves shared.example.com itself, so it never forwards it.
	require.NotContains(t, view.HTTP.Routers, "docker-02_shared")
	require.Equal(t, "docker-02", view.HTTP.Routers["docker-02_two"].Service)
	require.Equal(t, []string{"websecure"}, view.HTTP.Routers["docker-02_two"].EntryPoints)

	// ha.example.com runs on docker-02 and docker-03: balance over both.
	require.NotContains(t, view.HTTP.Routers, "docker-03_ha")
	ha := view.HTTP.Routers["docker-02_ha"]
	require.Equal(t, "lb_docker-02_ha", ha.Service)
	lb := view.HTTP.Services["lb_docker-02_ha"].LoadBalancer
	require.Equal(t, []dynamic.Server{{URL: "https://10.8.0.26:443"}, {URL: "https://10.8.0.27:443"}}, lb.Servers)
	require.Equal(t, "docker-02_ingress", lb.ServersTransport)
	require.True(t, view.HTTP.ServersTransports["docker-02_ingress"].InsecureSkipVerify)
	require.True(t, *lb.PassHostHeader)
}

func TestBuildViewKeepsFirstPeerWhenBackendsDiffer(t *testing.T) {
	peers := map[string]dynamic.Configuration{
		"docker-02": ingressPeer("docker-02", "10.8.0.26", map[string]string{"ha": "Host(`ha.example.com`)"}),
		"docker-03": ingressPeer("docker-03", "10.8.0.27", map[string]string{"ha": "Host(`ha.example.com`)"}),
	}
	peers["docker-03"].HTTP.ServersTransports["docker-03_ingress"].InsecureSkipVerify = false

	view := buildView(dynamic.Configuration{}, peers)
	require.Equal(t, "docker-02", view.HTTP.Routers["docker-02_ha"].Service)
	require.NotContains(t, view.HTTP.Routers, "docker-03_ha")
}

func TestBuildViewTCPAndUDP(t *testing.T) {
	tcp := func(rule string) dynamic.Configuration {
		return dynamic.Configuration{
			TCP: &dynamic.TCPConfiguration{
				Routers:  map[string]*dynamic.TCPRouter{"db@docker": {Rule: rule, Service: "db"}},
				Services: map[string]*dynamic.TCPService{"db@docker": {LoadBalancer: &dynamic.TCPServersLoadBalancer{}}},
			},
			UDP: &dynamic.UDPConfiguration{
				Routers: map[string]*dynamic.UDPRouter{"dns@docker": {Service: "dns"}},
			},
		}
	}
	peers := map[string]dynamic.Configuration{
		"docker-02": tcp("HostSNI(`a`)"),
		"docker-03": tcp("HostSNI(`a`)"),
	}
	view := buildView(dynamic.Configuration{}, peers)
	require.Len(t, view.TCP.Routers, 1) // first peer wins
	require.Contains(t, view.TCP.Routers, "db")
	require.Contains(t, view.UDP.Routers, "dns")

	view = buildView(tcp("HostSNI(`a`)"), peers)
	require.Empty(t, view.TCP.Routers)
	require.Empty(t, view.UDP.Routers)
}

// cluster simulates kop instances that share one Redis.
type cluster struct {
	t      *testing.T
	redis  *miniredis.Miniredis
	client *redis.Client
}

type clusterHost struct {
	config   Config
	store    *RedisStore
	registry *peerRegistry
	view     *viewBuilder
	handle   func(dynamic.Configuration)
}

func newCluster(t *testing.T) *cluster {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })
	return &cluster{t: t, redis: server, client: client}
}

func (c *cluster) host(config Config) *clusterHost {
	h := &clusterHost{config: config}
	h.store = newRedisStore(c.client, config.Hostname, defaultRootKey, config.RedisTTL)
	h.registry = newPeerRegistry(c.client, config.Hostname, config.RedisTTL)
	h.view = newViewBuilder(c.client, h.registry, config.Hostname, config.RedisTTL)
	h.handle = createConfigHandler(config, h.store, &docker.Provider{}, createTestClient(map[string]string{}), func(conf dynamic.Configuration) {
		require.NoError(c.t, h.registry.publish(config.Hostname, conf))
		h.view.setOwn(conf)
	})
	return h
}

func dockerRoutes(rules map[string]string) dynamic.Configuration {
	routers := make(map[string]*dynamic.Router)
	for name, rule := range rules {
		routers[name+"@docker"] = &dynamic.Router{Rule: rule, Service: name, TLS: &dynamic.RouterTLSConfig{}}
	}
	return dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{Routers: routers},
		TCP:  &dynamic.TCPConfiguration{},
		UDP:  &dynamic.UDPConfiguration{},
	}
}

func TestViewsContainOnlyOtherHosts(t *testing.T) {
	c := newCluster(t)
	ctx := context.Background()
	hosts := []*clusterHost{
		c.host(Config{Hostname: "docker-01", BindIP: "10.8.0.25", DefaultPort: 443, RedisTTL: 300}),
		c.host(Config{Hostname: "docker-02", BindIP: "10.8.0.26", DefaultPort: 443, RedisTTL: 300}),
		c.host(Config{Hostname: "docker-03", BindIP: "10.8.0.27", DefaultPort: 443, RedisTTL: 300, DisableSharedRoot: true}),
	}
	// No view before the local routes are known.
	hosts[0].view.rebuild(ctx)
	require.False(t, c.redis.Exists("traefik-docker-01_kop_last_update@docker-01"))

	hosts[0].handle(dockerRoutes(map[string]string{"one": "Host(`one.example.com`)"}))
	hosts[1].handle(dockerRoutes(map[string]string{"two": "Host(`two.example.com`)"}))
	hosts[2].handle(dockerRoutes(map[string]string{"three": "Host(`three.example.com`)"}))
	for _, h := range hosts {
		h.view.rebuild(ctx)
	}

	get := func(key string) string {
		v, _ := c.redis.Get(key)
		return v
	}
	// Each view holds the routes of the other hosts only.
	require.Empty(t, get("traefik-docker-01/http/routers/docker-01_one/rule"))
	require.Empty(t, get("traefik-docker-01/http/services/docker-01/loadBalancer/servers/0/url"))
	require.Empty(t, get("traefik-docker-01/http/serversTransports/docker-01_ingress/insecureSkipVerify"))
	require.Equal(t, "Host(`two.example.com`)", get("traefik-docker-01/http/routers/docker-02_two/rule"))
	require.Equal(t, "Host(`three.example.com`)", get("traefik-docker-01/http/routers/docker-03_three/rule"))
	require.Equal(t, "https://10.8.0.27:443", get("traefik-docker-01/http/services/docker-03/loadBalancer/servers/0/url"))
	require.Equal(t, "true", get("traefik-docker-01/http/serversTransports/docker-03_ingress/insecureSkipVerify"))
	require.Equal(t, "docker-02_ingress", get("traefik-docker-01/http/services/docker-02/loadBalancer/serversTransport"))
	require.Equal(t, "Host(`one.example.com`)", get("traefik-docker-03/http/routers/docker-01_one/rule"))
	require.Empty(t, get("traefik-docker-03/http/routers/docker-03_three/rule"))

	// The shared root keeps all hosts that publish to it.
	require.Equal(t, "Host(`one.example.com`)", get("traefik/http/routers/docker-01_one/rule"))
	require.Equal(t, "Host(`two.example.com`)", get("traefik/http/routers/docker-02_two/rule"))
	require.Empty(t, get("traefik/http/routers/docker-03_three/rule"))

	// Unchanged configurations do not rewrite anything.
	c.redis.Del("traefik-docker-01/http/routers/docker-02_two/rule")
	hosts[0].view.rebuild(ctx)
	require.Empty(t, get("traefik-docker-01/http/routers/docker-02_two/rule"))
	require.NoError(t, hosts[0].view.KeepConfAlive()) // repairs lost keys
	require.Equal(t, "Host(`two.example.com`)", get("traefik-docker-01/http/routers/docker-02_two/rule"))

	// A route that moves away from a host disappears from the other views.
	hosts[1].handle(dockerRoutes(map[string]string{}))
	hosts[0].view.rebuild(ctx)
	require.Empty(t, get("traefik-docker-01/http/routers/docker-02_two/rule"))
	require.Empty(t, get("traefik-docker-01/http/services/docker-02/loadBalancer/servers/0/url"))

	// A host that stops renewing its configuration drops out after the TTL.
	c.redis.FastForward(200 * time.Second)
	for _, h := range hosts[:2] {
		require.NoError(t, h.registry.KeepConfAlive())
		require.NoError(t, h.view.KeepConfAlive())
	}
	c.redis.FastForward(200 * time.Second)
	hosts[0].view.rebuild(ctx)
	require.Empty(t, get("traefik-docker-01/http/routers/docker-03_three/rule"))
}

func TestViewFollowsPeerNotifications(t *testing.T) {
	c := newCluster(t)
	one := c.host(Config{Hostname: "docker-01", BindIP: "10.8.0.25", DefaultPort: 443})
	two := c.host(Config{Hostname: "docker-02", BindIP: "10.8.0.26", DefaultPort: 443})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		one.view.run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	one.handle(dockerRoutes(map[string]string{"one": "Host(`one.example.com`)"}))
	require.Eventually(t, func() bool { return c.redis.Exists("traefik-docker-01_kop_last_update@docker-01") }, 2*time.Second, 10*time.Millisecond)

	// The subscription may start after the first publish; retry until it is live.
	require.Eventually(t, func() bool {
		two.registry.mu.Lock()
		delete(two.registry.last, "docker-02") // force a new notification
		two.registry.mu.Unlock()
		two.handle(dockerRoutes(map[string]string{"two": "Host(`two.example.com`)"}))
		v, _ := c.redis.Get("traefik-docker-01/http/routers/docker-02_two/rule")
		return v == "Host(`two.example.com`)"
	}, 3*time.Second, 50*time.Millisecond)
}

func TestImportTraefikAPI(t *testing.T) {
	pages := map[string][]apiRouter{
		"1": {
			{Name: "whoami-abc@kubernetescrd", Provider: "kubernetescrd", Status: "enabled", Rule: "Host(`whoami.example.com`)", TLS: &dynamic.RouterTLSConfig{CertResolver: "le"}},
			{Name: "dashboard@internal", Provider: "internal", Status: "enabled", Rule: "PathPrefix(`/api`)"},
		},
		"2": {
			{Name: "docker-01_one@redis", Provider: "redis", Status: "enabled", Rule: "Host(`one.example.com`)"},
			{Name: "broken@kubernetes", Provider: "kubernetes", Status: "disabled", Rule: "Host(`broken.example.com`)"},
			{Name: "grafana@kubernetes", Provider: "kubernetes", Status: "warning", Rule: "Host(`grafana.example.com`)", Priority: 7},
		},
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/http/routers", r.URL.Path)
		page := r.URL.Query().Get("page")
		if page == "1" {
			w.Header().Set("X-Next-Page", "2")
		} else {
			w.Header().Set("X-Next-Page", "1")
		}
		require.NoError(t, json.NewEncoder(w).Encode(pages[page]))
	}))
	t.Cleanup(api.Close)

	c := newCluster(t)
	one := c.host(Config{Hostname: "docker-01", BindIP: "10.8.0.25", DefaultPort: 443})
	config := Config{
		Hostname:         "docker-02",
		ImportTraefikAPI: api.URL + "/api/",
		ImportName:       "k3s",
		ImportBackend:    "https://10.8.0.10:443",
	}
	importer, err := newTraefikAPIImporter(config, newPeerRegistry(c.client, config.Hostname, 0))
	require.NoError(t, err)
	require.NoError(t, importer.importOnce(context.Background()))

	one.handle(dockerRoutes(map[string]string{"one": "Host(`one.example.com`)"}))
	one.view.rebuild(context.Background())

	view, err := one.view.store.Gets("traefik-docker-01/http/*")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"traefik-docker-01/http/routers/k3s_whoami-abc-kubernetescrd/rule":          "Host(`whoami.example.com`)",
		"traefik-docker-01/http/routers/k3s_whoami-abc-kubernetescrd/service":       "k3s",
		"traefik-docker-01/http/routers/k3s_whoami-abc-kubernetescrd/entryPoints/0": "websecure",
		"traefik-docker-01/http/routers/k3s_whoami-abc-kubernetescrd/tls":           "true",
		"traefik-docker-01/http/routers/k3s_grafana-kubernetes/rule":                "Host(`grafana.example.com`)",
		"traefik-docker-01/http/routers/k3s_grafana-kubernetes/service":             "k3s",
		"traefik-docker-01/http/routers/k3s_grafana-kubernetes/priority":            "7",
		"traefik-docker-01/http/routers/k3s_grafana-kubernetes/entryPoints/0":       "websecure",
		"traefik-docker-01/http/routers/k3s_grafana-kubernetes/tls":                 "true",
		"traefik-docker-01/http/services/k3s/loadBalancer/servers/0/url":            "https://10.8.0.10:443",
		"traefik-docker-01/http/services/k3s/loadBalancer/passHostHeader":           "true",
		"traefik-docker-01/http/services/k3s/loadBalancer/serversTransport":         "k3s_ingress",
		"traefik-docker-01/http/serversTransports/k3s_ingress/insecureSkipVerify":   "true",
	}, view)

	// The import never reaches the shared root, which k3s may read itself.
	shared, err := one.store.Gets("traefik/http/routers/k3s*")
	require.NoError(t, err)
	require.Empty(t, shared)
}

func TestRedisStoreSkipsUnchangedConfiguration(t *testing.T) {
	server, store, conf := newTTLTestStore(t, 0)
	require.NoError(t, store.Store(conf))
	key := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	server.Del(key)
	require.NoError(t, store.Store(*conf.DeepCopy()))
	require.False(t, server.Exists(key))
	require.NoError(t, store.KeepConfAlive()) // repairs lost keys without a TTL
	require.True(t, server.Exists(key))
}
