package traefikkop

import (
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

var ingressHostname = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*$`)

// buildHTTPIngress publishes only the routing decisions that k3s needs.
// The Docker ingress handles the original route, including its middlewares.
func buildHTTPIngress(dc *dockerCache, docker *dynamic.HTTPConfiguration, config Config) *dynamic.HTTPConfiguration {
	http := &dynamic.HTTPConfiguration{
		Routers:  make(map[string]*dynamic.Router),
		Services: make(map[string]*dynamic.Service),
	}
	if docker == nil {
		return http
	}

	for name, router := range docker.Routers {
		name = stripDocker(name)
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
			http.Services[service] = &dynamic.Service{
				LoadBalancer: &dynamic.ServersLoadBalancer{
					Servers:        []dynamic.Server{{URL: "https://" + net.JoinHostPort(strings.Trim(config.BindIP, "[]"), port)}},
					PassHostHeader: new(true),
				},
			}
		}
		http.Routers[config.Hostname+"_"+name] = &dynamic.Router{
			Rule:     router.Rule,
			Priority: router.Priority,
			Service:  service,
		}
	}
	return http
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
