package traefikkop

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	kvredis "github.com/kvtools/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/config/kv"
)

// readAsTraefik reads root the way Traefik's Redis provider does: it lists all
// keys starting with root (which includes other roots such as traefik-<host>)
// and decodes them.
func readAsTraefik(t *testing.T, addr, root string) *dynamic.Configuration {
	t.Helper()
	ctx := context.Background()
	s, err := kvredis.New(ctx, []string{addr}, nil)
	require.NoError(t, err)
	pairs, err := s.List(ctx, root, nil)
	require.NoError(t, err)
	cfg := &dynamic.Configuration{}
	require.NoError(t, kv.Decode(pairs, cfg, root))
	return cfg
}

func TestTraefikReadsSharedRootAndViewsSeparately(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { client.Close() })

	shared := dockerRoutes(map[string]string{"one": "Host(`one.example.com`)"})
	require.NoError(t, newRedisStore(client, "docker-01", defaultRootKey, 0).Store(shared))
	view := dockerRoutes(map[string]string{"two": "Host(`two.example.com`)"})
	require.NoError(t, newRedisStore(client, "docker-0", viewRootKey("docker-0"), 0).Store(view))
	other := dockerRoutes(map[string]string{"three": "Host(`three.example.com`)"})
	require.NoError(t, newRedisStore(client, "docker-01", viewRootKey("docker-01"), 0).Store(other))

	cfg := readAsTraefik(t, server.Addr(), "traefik")
	require.Equal(t, []string{"one"}, sortedKeys(cfg.HTTP.Routers))

	// traefik-docker-0 is a prefix of traefik-docker-01, yet both stay separate.
	cfg = readAsTraefik(t, server.Addr(), viewRootKey("docker-0"))
	require.Equal(t, []string{"two"}, sortedKeys(cfg.HTTP.Routers))
	require.Equal(t, "Host(`two.example.com`)", cfg.HTTP.Routers["two"].Rule)
	cfg = readAsTraefik(t, server.Addr(), viewRootKey("docker-01"))
	require.Equal(t, []string{"three"}, sortedKeys(cfg.HTTP.Routers))
}
