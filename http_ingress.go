package traefikkop

import (
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

var ingressHostname = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*$`)

const defaultIngressEntryPoint = "websecure"

// buildHTTPIngress publishes only the routing decisions that k3s needs.
// The Docker ingress handles the original route, including its middlewares.
func buildHTTPIngress(dc *dockerCache, docker *dynamic.HTTPConfiguration, config Config) *dynamic.HTTPConfiguration {
	http := &dynamic.HTTPConfiguration{
		Routers:           make(map[string]*dynamic.Router),
		Services:          make(map[string]*dynamic.Service),
		ServersTransports: make(map[string]*dynamic.ServersTransport),
	}
	if docker == nil {
		return http
	}
	transport := addIngressTransport(http, config.Hostname, config)

	for _, dockerName := range selectIngressRouters(docker) {
		router := docker.Routers[dockerName]
		name := stripDocker(dockerName)
		service := config.Hostname
		port := strconv.Itoa(config.DefaultPort)
		if override := getHTTPPortOverride(dc, router.Service, name); override != "" {
			port = override
			app := stripDocker(router.Service)
			if app == "" || strings.Contains(app, "@") {
				app = name
			}
			service += "_" + app + "_" + port
		}

		if _, exists := http.Services[service]; !exists {
			url := "https://" + net.JoinHostPort(strings.Trim(config.BindIP, "[]"), port)
			http.Services[service] = ingressService(url, transport)
		}
		http.Routers[config.Hostname+"_"+name] = ingressRouter(router, service, config)
	}
	return http
}

// ingressRouter forwards matching requests to service on the ingress entrypoints.
func ingressRouter(router *dynamic.Router, service string, config Config) *dynamic.Router {
	entryPoints := []string{defaultIngressEntryPoint}
	if len(config.IngressEntryPoints) > 0 {
		entryPoints = append([]string(nil), config.IngressEntryPoints...)
	}
	return &dynamic.Router{
		EntryPoints: entryPoints,
		TLS:         &dynamic.RouterTLSConfig{},
		Rule:        router.Rule,
		RuleSyntax:  router.RuleSyntax,
		Priority:    router.Priority,
		Service:     service,
	}
}

// ingressService forwards to url and keeps the incoming Host header, so the
// receiving Traefik routes the request with its own rules.
func ingressService(url string, transport string) *dynamic.Service {
	return &dynamic.Service{
		LoadBalancer: &dynamic.ServersLoadBalancer{
			Servers:          []dynamic.Server{{URL: url}},
			PassHostHeader:   new(true),
			ServersTransport: transport,
		},
	}
}

// addIngressTransport adds the serversTransport used to reach the receiving
// Traefik and returns its name. The backend is addressed by IP, so its
// certificate cannot match unless verification is disabled.
func addIngressTransport(http *dynamic.HTTPConfiguration, owner string, config Config) string {
	if config.IngressVerifyBackendCerts {
		return ""
	}
	name := owner + "_ingress"
	http.ServersTransports[name] = &dynamic.ServersTransport{InsecureSkipVerify: true}
	return name
}

// selectIngressRouters returns one router name per rule, sorted by name.
// Setups often pair a TLS router with an HTTP router that only redirects to
// HTTPS. Both would land on the same ingress entrypoint with the same rule, so
// keep the TLS router, then a router without a redirect, then the first name.
func selectIngressRouters(http *dynamic.HTTPConfiguration) []string {
	names := make([]string, 0, len(http.Routers))
	for name := range http.Routers {
		names = append(names, name)
	}
	sort.Strings(names)

	best := make(map[string]string) // rule -> router name
	for _, name := range names {
		rule := http.Routers[name].Rule
		current, ok := best[rule]
		if !ok || ingressRouterScore(http, name) > ingressRouterScore(http, current) {
			best[rule] = name
		}
	}

	selected := make([]string, 0, len(best))
	for _, name := range best {
		selected = append(selected, name)
	}
	sort.Strings(selected)
	return selected
}

func ingressRouterScore(http *dynamic.HTTPConfiguration, name string) int {
	router := http.Routers[name]
	score := 0
	if router.TLS != nil {
		score += 2
	}
	if !redirectsOnly(http, router) {
		score++
	}
	return score
}

// redirectsOnly reports whether the router uses a redirect middleware.
// Middlewares from other providers are recognized by name only.
func redirectsOnly(http *dynamic.HTTPConfiguration, router *dynamic.Router) bool {
	for _, mw := range router.Middlewares {
		if strings.Contains(strings.ToLower(mw), "redirect") {
			return true
		}
		for _, name := range []string{mw, mw + "@docker", stripDocker(mw)} {
			if m := http.Middlewares[name]; m != nil && (m.RedirectScheme != nil || m.RedirectRegex != nil) {
				return true
			}
		}
	}
	return false
}

// applyHTTPPortOverrides replaces the final port after Docker port mapping.
func applyHTTPPortOverrides(dc *dockerCache, conf *dynamic.Configuration) {
	if conf.HTTP == nil {
		return
	}
	for name, service := range conf.HTTP.Services {
		if service.LoadBalancer == nil {
			continue
		}
		override := getHTTPPortOverride(dc, name, getRouterOfService(conf, name, "http"))
		if override == "" {
			continue
		}
		for i := range service.LoadBalancer.Servers {
			server := &service.LoadBalancer.Servers[i]
			u, err := url.Parse(server.URL)
			if err != nil || u.Hostname() == "" {
				continue
			}
			u.Host = net.JoinHostPort(u.Hostname(), override)
			server.URL = u.String()
		}
	}
}

func httpPortOverrideLabel(name string) string {
	return "kop.traefik.http.services." + strings.ToLower(stripDocker(name)) + ".loadbalancer.server.port"
}

func getHTTPPortOverride(dc *dockerCache, service, router string) string {
	c, err := dc.findContainerByServiceName("http", service, router)
	if err != nil {
		return ""
	}
	service = stripDocker(service)
	if service == "" || strings.Contains(service, "@") {
		service = router
	}
	key := httpPortOverrideLabel(service)
	value := strings.TrimSpace(c.Config.Labels[key])
	if value == "" {
		return ""
	}
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		log.Warn().Str("label", key).Str("value", value).Msg("ignoring invalid kop backend port")
		return ""
	}
	return strconv.FormatUint(port, 10)
}
