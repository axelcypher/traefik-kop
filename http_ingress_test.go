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
