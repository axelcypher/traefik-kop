package traefikkop

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/provider/docker"
)

func TestHTTPIngressPublishesSeparateHostRoutes(t *testing.T) {
	redis := miniredis.RunT(t)
	store1 := NewRedisStore("docker-01", redis.Addr(), 0, "", "", 0, nil, "", nil)
	store2 := NewRedisStore("docker-02", redis.Addr(), 0, "", "", 0, nil, "", nil)
	client := createTestClient(map[string]string{
		"traefik.docker.network":                             "proxy",
		"traefik.http.routers.dashboard.service":             "api@internal",
		"traefik.http.services.app.loadbalancer.server.port": "8080",
		httpPortOverrideLabel("app"):                         "123",
	})

	conf := dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers: map[string]*dynamic.Router{
				"traefik-traefik@docker": {Rule: "Host(`traefik-01.example.com`)", Service: "traefik-traefik"},
				"dashboard@docker": {
					Rule:        "Host(`dashboard.example.com`)",
					Priority:    10,
					EntryPoints: []string{"https"},
					Middlewares: []string{"strip"},
					Service:     "api@internal",
					TLS:         &dynamic.RouterTLSConfig{CertResolver: "dns-cloudflare"},
				},
				"app@docker": {Rule: "Host(`app.example.com`)", Service: "app"},
			},
			Services: map[string]*dynamic.Service{
				"app@docker": {LoadBalancer: &dynamic.ServersLoadBalancer{
					Servers: []dynamic.Server{{URL: "http://172.18.0.2:8080"}},
				}},
			},
			Middlewares: map[string]*dynamic.Middleware{
				"strip@docker": {StripPrefix: &dynamic.StripPrefix{Prefixes: []string{"/app"}}},
			},
		},
		TCP: &dynamic.TCPConfiguration{},
		UDP: &dynamic.UDPConfiguration{},
	}

	config := Config{Hostname: "docker-01", BindIP: "10.8.0.25", DefaultPort: 443}
	require.NoError(t, store1.Store(*conf.DeepCopy()))
	handle1 := createConfigHandler(config, store1, &docker.Provider{}, client)
	handle1(*conf.DeepCopy())

	config.Hostname = "docker-02"
	config.BindIP = "10.8.0.26"
	conf.HTTP.Routers["traefik-traefik@docker"].Rule = "Host(`traefik-02.example.com`)"
	delete(conf.HTTP.Routers, "dashboard@docker")
	delete(conf.HTTP.Routers, "app@docker")
	createConfigHandler(config, store2, &docker.Provider{}, client)(conf)

	require.Equal(t, "Host(`traefik-01.example.com`)", g(store1, "traefik/http/routers/docker-01_traefik-traefik/rule"))
	require.Equal(t, "Host(`traefik-02.example.com`)", g(store1, "traefik/http/routers/docker-02_traefik-traefik/rule"))
	require.Equal(t, "docker-02", g(store1, "traefik/http/routers/docker-02_traefik-traefik/service"))
	require.Equal(t, "docker-01", g(store1, "traefik/http/routers/docker-01_traefik-traefik/service"))
	require.Equal(t, "docker-01_app_123", g(store1, "traefik/http/routers/docker-01_app/service"))
	assertServiceIPs(t, store1, []svc{
		{"docker-01", "http", "https://10.8.0.25:443"},
		{"docker-01_app_123", "http", "https://10.8.0.25:123"},
		{"docker-02", "http", "https://10.8.0.26:443"},
	})

	dashboard := "traefik/http/routers/docker-01_dashboard/"
	keys, err := store1.Gets(dashboard + "*")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		dashboard + "rule":          "Host(`dashboard.example.com`)",
		dashboard + "priority":      "10",
		dashboard + "entryPoints/0": "websecure",
		dashboard + "tls":           "true",
		dashboard + "service":       "docker-01",
	}, keys)
	require.False(t, redis.Exists("traefik/http/middlewares/strip/stripPrefix/prefixes/0"))
	require.False(t, redis.Exists("traefik/http/services/app/loadBalancer/servers/0/url"))
	require.False(t, redis.Exists("traefik/http/routers/traefik-traefik/rule"))

	conf.HTTP = &dynamic.HTTPConfiguration{}
	handle1(conf)
	require.Empty(t, g(store1, "traefik/http/routers/docker-01_traefik-traefik/rule"))
	require.Empty(t, g(store1, "traefik/http/services/docker-01/loadBalancer/servers/0/url"))
	require.Equal(t, "Host(`traefik-02.example.com`)", g(store1, "traefik/http/routers/docker-02_traefik-traefik/rule"))
	require.Equal(t, "https://10.8.0.26:443", g(store1, "traefik/http/services/docker-02/loadBalancer/servers/0/url"))
}

func TestStandaloneHTTPPortOverrideUsesFinalPort(t *testing.T) {
	client := createTestClient(map[string]string{
		"traefik.http.services.app.loadbalancer.server.port": "80",
		httpPortOverrideLabel("app"):                         "123",
	})
	client.container.HostConfig.PortBindings = nat.PortMap{
		"80/tcp":  {{HostPort: "8080"}},
		"123/tcp": {{HostPort: "8123"}},
	}
	conf := dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers: map[string]*dynamic.Router{"app@docker": {Service: "app"}},
			Services: map[string]*dynamic.Service{
				"app@docker": {LoadBalancer: &dynamic.ServersLoadBalancer{
					Servers: []dynamic.Server{{URL: "http://172.18.0.2:80"}},
				}},
			},
		},
		TCP: &dynamic.TCPConfiguration{},
		UDP: &dynamic.UDPConfiguration{},
	}
	store := &testStore{}
	createConfigHandler(Config{BindIP: "10.8.0.25"}, store, &docker.Provider{}, client)(conf)
	assertServiceIPs(t, store, []svc{{"app", "http", "http://10.8.0.25:123"}})
	require.Equal(t, "app", g(store, "traefik/http/routers/app/service"))
}

func TestHTTPIngressRefreshesKopOnlyPortChanges(t *testing.T) {
	client := createTestClient(map[string]string{
		"traefik.http.services.unifi-network.loadbalancer.server.port": "8443",
	})
	conf := dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers: map[string]*dynamic.Router{
				"unifi-network@docker": {Service: "unifi-network", Rule: "Host(`unifi.example.com`)"},
			},
			Services: map[string]*dynamic.Service{
				"unifi-network@docker": {LoadBalancer: &dynamic.ServersLoadBalancer{
					Servers: []dynamic.Server{{URL: "https://172.18.0.2:8443"}},
				}},
			},
		},
		TCP: &dynamic.TCPConfiguration{},
		UDP: &dynamic.UDPConfiguration{},
	}
	redis := miniredis.RunT(t)
	store := NewRedisStore("vm-docker-01", redis.Addr(), 0, "", "", 0, nil, "", nil)
	refresh := &configurationRefresh{handle: createConfigHandler(
		Config{Hostname: "vm-docker-01", BindIP: "10.8.0.25", DefaultPort: 443},
		store, &docker.Provider{}, client,
	)}
	refresh.Refresh() // No provider configuration received yet.
	refresh.Update(conf)
	key := "traefik/http/routers/vm-docker-01_unifi-network/service"
	require.Equal(t, "vm-docker-01", g(store, key))

	// Traefik's configuration stays identical when only a kop label changes.
	client.container.Config.Labels[httpPortOverrideLabel("unifi-network")] = "8443"
	refresh.Refresh()
	require.Equal(t, "vm-docker-01_unifi-network_8443", g(store, key))
	assertServiceIPs(t, store, []svc{{"vm-docker-01_unifi-network_8443", "http", "https://10.8.0.25:8443"}})
	require.Equal(t, "websecure", g(store, "traefik/http/routers/vm-docker-01_unifi-network/entryPoints/0"))

	delete(client.container.Config.Labels, httpPortOverrideLabel("unifi-network"))
	refresh.Refresh()
	require.Equal(t, "vm-docker-01", g(store, key))
	require.Empty(t, g(store, "traefik/http/services/vm-docker-01_unifi-network_8443/loadBalancer/servers/0/url"))
	assertServiceIPs(t, store, []svc{{"vm-docker-01", "http", "https://10.8.0.25:443"}})
	require.Equal(t, "unifi-network", conf.HTTP.Routers["unifi-network@docker"].Service)
}

func TestHTTPIngressTransportAndEntryPoints(t *testing.T) {
	client := createTestClient(map[string]string{})
	conf := dynamic.Configuration{
		HTTP: &dynamic.HTTPConfiguration{
			Routers: map[string]*dynamic.Router{
				"app@docker": {Rule: "Host(`app.example.com`)", Service: "app", RuleSyntax: "v2"},
			},
			Services: map[string]*dynamic.Service{},
		},
		TCP: &dynamic.TCPConfiguration{},
		UDP: &dynamic.UDPConfiguration{},
	}

	store := &testStore{}
	config := Config{Hostname: "docker-01", BindIP: "10.8.0.25", DefaultPort: 443}
	createConfigHandler(config, store, &docker.Provider{}, client)(*conf.DeepCopy())
	require.Equal(t, "true", g(store, "traefik/http/serversTransports/docker-01_ingress/insecureSkipVerify"))
	require.Equal(t, "docker-01_ingress", g(store, "traefik/http/services/docker-01/loadBalancer/serversTransport"))
	require.Equal(t, "true", g(store, "traefik/http/services/docker-01/loadBalancer/passHostHeader"))
	require.Equal(t, "websecure", g(store, "traefik/http/routers/docker-01_app/entryPoints/0"))
	require.Equal(t, "v2", g(store, "traefik/http/routers/docker-01_app/ruleSyntax"))

	store = &testStore{}
	config.IngressVerifyBackendCerts = true
	config.IngressEntryPoints = []string{"https", "internal"}
	createConfigHandler(config, store, &docker.Provider{}, client)(*conf.DeepCopy())
	require.Empty(t, g(store, "traefik/http/serversTransports/docker-01_ingress/insecureSkipVerify"))
	require.Empty(t, g(store, "traefik/http/services/docker-01/loadBalancer/serversTransport"))
	require.Equal(t, "https", g(store, "traefik/http/routers/docker-01_app/entryPoints/0"))
	require.Equal(t, "internal", g(store, "traefik/http/routers/docker-01_app/entryPoints/1"))
}

func TestHTTPIngressPublishesOneRouterPerRule(t *testing.T) {
	http := &dynamic.HTTPConfiguration{
		Routers: map[string]*dynamic.Router{
			// HTTP router that only redirects, sorted before its TLS twin.
			"a-http@docker":  {Rule: "Host(`a.example.com`)", Middlewares: []string{"to-https"}},
			"b-https@docker": {Rule: "Host(`a.example.com`)", TLS: &dynamic.RouterTLSConfig{}},
			// Neither uses TLS: skip the redirect, recognized by its definition.
			"c-plain@docker": {Rule: "Host(`c.example.com`)", Middlewares: []string{"https-only@docker"}},
			"d-plain@docker": {Rule: "Host(`c.example.com`)"},
			// Same score: the first name wins.
			"e@docker": {Rule: "Host(`e.example.com`)"},
			"f@docker": {Rule: "Host(`e.example.com`)"},
			// Redirect middleware from another provider, recognized by name.
			"g@docker": {Rule: "Host(`g.example.com`)", Middlewares: []string{"redirect-to-https@file"}},
			"h@docker": {Rule: "Host(`g.example.com`)"},
		},
		Middlewares: map[string]*dynamic.Middleware{
			"to-https@docker":   {RedirectScheme: &dynamic.RedirectScheme{Scheme: "https"}},
			"https-only@docker": {RedirectScheme: &dynamic.RedirectScheme{Scheme: "https"}},
		},
	}
	require.Equal(t, []string{"b-https@docker", "d-plain@docker", "e@docker", "h@docker"}, selectIngressRouters(http))
}
