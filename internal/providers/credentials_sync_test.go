package providers

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enterpilot/gomodel/config"
	"github.com/enterpilot/gomodel/internal/core"
	"github.com/enterpilot/gomodel/internal/storage/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type credentialSyncProvider struct {
	*registryMockProvider
	cfg     ProviderConfig
	keys    *Keyring
	entered chan struct{}
	release chan struct{}
}

func (p *credentialSyncProvider) ListModels(ctx context.Context) (*core.ModelsResponse, error) {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.modelsResponse, p.err
}

func (p *credentialSyncProvider) ChatCompletion(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &core.ChatResponse{ID: p.cfg.APIKeys[0]}, nil
}

func newCredentialSyncFactory(t *testing.T, beforeBuild func(ProviderConfig)) (*ProviderFactory, *atomic.Int64) {
	t.Helper()
	factory := NewProviderFactory()
	builds := &atomic.Int64{}
	factory.Add(Registration{
		Type: "test",
		New: func(cfg ProviderConfig, options ProviderOptions) core.Provider {
			builds.Add(1)
			if beforeBuild != nil {
				beforeBuild(cfg)
			}
			models := make([]core.Model, 0, len(cfg.Models))
			for _, id := range cfg.Models {
				models = append(models, core.Model{ID: id, Object: "model", OwnedBy: "test"})
			}
			if len(models) == 0 && len(cfg.APIKeys) > 0 && cfg.APIKeys[0] == "good-list" {
				models = append(models, core.Model{ID: "old-upstream-model", Object: "model", OwnedBy: "test"})
			}
			provider := &credentialSyncProvider{
				registryMockProvider: &registryMockProvider{
					name:           cfg.Name,
					modelsResponse: &core.ModelsResponse{Object: "list", Data: models},
				},
				cfg:  cfg,
				keys: options.Keys,
			}
			if len(cfg.APIKeys) > 0 && cfg.APIKeys[0] == "list-fails" {
				provider.err = errors.New("model inventory unavailable")
			}
			return provider
		},
	})
	return factory, builds
}

type failingCredentialListStore struct {
	CredentialStore
	mu  sync.RWMutex
	err error
}

func (s *failingCredentialListStore) List(ctx context.Context) ([]ManagedProviderCredential, error) {
	s.mu.RLock()
	err := s.err
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return s.CredentialStore.List(ctx)
}

func (s *failingCredentialListStore) fail(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func newCredentialSyncSQLStores(t *testing.T) (*SQLCredentialStore, *SQLCredentialStore) {
	t.Helper()
	dsn := "file:credential_sync_" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	open := func() *sql.DB {
		raw, err := sql.Open("sqlite", dsn)
		require.NoError(t, err)
		raw.SetMaxOpenConns(1)
		t.Cleanup(func() { assert.NoError(t, raw.Close()) })
		return raw
	}
	dbA, err := sqlx.NewSQLite(open())
	require.NoError(t, err)
	storeA, err := NewSQLCredentialStore(t.Context(), dbA)
	require.NoError(t, err)
	dbB, err := sqlx.NewSQLite(open())
	require.NoError(t, err)
	storeB, err := NewSQLCredentialStore(t.Context(), dbB)
	require.NoError(t, err)
	return storeA, storeB
}

func TestCredentialsServiceReconcilesSharedSQLStoreChanges(t *testing.T) {
	ctx := t.Context()
	storeA, rawStoreB := newCredentialSyncSQLStores(t)
	storeB := &failingCredentialListStore{CredentialStore: rawStoreB}
	factory, builds := newCredentialSyncFactory(t, nil)
	registryA, registryB := NewModelRegistry(), NewModelRegistry()
	registryA.SetConfiguredProviderModelsMode(config.ConfiguredProviderModelsModeAllowlist)
	registryB.SetConfiguredProviderModelsMode(config.ConfiguredProviderModelsModeAllowlist)
	serviceA, err := NewCredentialsService(ctx, factory, registryA, storeA, nil, config.ResilienceConfig{}, nil)
	require.NoError(t, err)
	serviceB, err := NewCredentialsService(ctx, factory, registryB, storeB, nil, config.ResilienceConfig{}, nil)
	require.NoError(t, err)

	row := ManagedProviderCredential{
		Name: "shared", Type: "test", APIKeys: []string{"key-a1", "key-a2"},
		Models: []string{"model-a", "model-b"}, Enabled: true,
	}
	require.NoError(t, serviceA.Upsert(ctx, row))
	assert.True(t, registryA.Supports("shared/model-a"))
	require.NoError(t, serviceB.Reconcile(ctx))
	providerB, ok := registryB.ProviderByName("shared").(*credentialSyncProvider)
	require.True(t, ok)
	assert.Equal(t, []string{"key-a1", "key-a2"}, providerB.cfg.APIKeys)
	assert.True(t, registryB.Supports("shared/model-a"))

	buildsBefore := builds.Load()
	require.NoError(t, serviceB.Reconcile(ctx))
	assert.Same(t, providerB, registryB.ProviderByName("shared"), "unchanged rows must keep the installed adapter")
	assert.Equal(t, buildsBefore, builds.Load(), "unchanged rows must not reconstruct providers")

	// A request already holding the old adapter must finish while future lookups
	// switch atomically to the replacement.
	providerB.entered = make(chan struct{}, 1)
	providerB.release = make(chan struct{})
	call := make(chan *core.ChatResponse, 1)
	callErr := make(chan error, 1)
	go func() {
		response, err := providerB.ChatCompletion(ctx, &core.ChatRequest{})
		call <- response
		callErr <- err
	}()
	select {
	case <-providerB.entered:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "in-flight request did not reach the old adapter")
	}

	row.APIKeys = []string{"key-b1", "key-b2"}
	row.Models = []string{"model-b", "model-c"}
	require.NoError(t, storeA.Upsert(ctx, row)) // another replica/dashboard wrote the shared DB
	stopWatch, watchDone := make(chan struct{}), make(chan struct{})
	var missing atomic.Int64
	go func() {
		defer close(watchDone)
		for {
			select {
			case <-stopWatch:
				return
			default:
				if registryB.ProviderByName("shared") == nil || !registryB.Supports("shared/model-b") {
					missing.Add(1)
				}
				runtime.Gosched()
			}
		}
	}()
	require.NoError(t, serviceB.Reconcile(ctx))
	close(stopWatch)
	<-watchDone
	assert.Zero(t, missing.Load(), "provider replacement must never create an unregistered interval")
	providerBNext, ok := registryB.ProviderByName("shared").(*credentialSyncProvider)
	require.True(t, ok)
	assert.NotSame(t, providerB, providerBNext)
	assert.Equal(t, []string{"key-b1", "key-b2"}, providerBNext.cfg.APIKeys)
	assert.False(t, registryB.Supports("shared/model-a"))
	assert.True(t, registryB.Supports("shared/model-b"))
	assert.True(t, registryB.Supports("shared/model-c"))

	close(providerB.release)
	require.NoError(t, <-callErr)
	assert.Equal(t, "key-a1", (<-call).ID, "the captured adapter remains usable for in-flight work")

	storeB.fail(errors.New("temporary list failure"))
	assert.Error(t, serviceB.Reconcile(ctx))
	assert.Same(t, providerBNext, registryB.ProviderByName("shared"), "a failed list must not change good state")
	storeB.fail(nil)

	// Invalid enabled rows are retried rather than acknowledged; the working
	// provider stays live until a valid row arrives.
	bad := row
	bad.APIKeys = nil
	require.NoError(t, storeA.Upsert(ctx, bad))
	assert.Error(t, serviceB.Reconcile(ctx))
	assert.Same(t, providerBNext, registryB.ProviderByName("shared"))
	assert.True(t, registryB.Supports("shared/model-c"))

	row.APIKeys = []string{"key-c1"}
	row.Models = []string{"model-c", "model-d"}
	require.NoError(t, storeA.Upsert(ctx, row))
	require.NoError(t, serviceB.Reconcile(ctx), "a later valid read retries the failed row")
	providerBNext, ok = registryB.ProviderByName("shared").(*credentialSyncProvider)
	require.True(t, ok)
	assert.Equal(t, []string{"key-c1"}, providerBNext.cfg.APIKeys)
	assert.False(t, registryB.Supports("shared/model-b"))
	assert.True(t, registryB.Supports("shared/model-d"))

	// Disabled rows are authoritative even when the stored credentials would
	// fail normal validation; the row remains persisted for later re-enabling.
	row.Type = "invalid-type"
	row.APIKeys = nil
	row.Models = nil
	row.Enabled = false
	require.NoError(t, storeA.Upsert(ctx, row))
	require.NoError(t, serviceB.Reconcile(ctx))
	assert.Nil(t, registryB.ProviderByName("shared"))
	stored, err := storeA.Get(ctx, "shared")
	require.NoError(t, err)
	assert.False(t, stored.Enabled)

	row.Type = "test"
	row.APIKeys = []string{"key-d"}
	row.Models = []string{"model-e"}
	row.Enabled = true
	require.NoError(t, storeA.Upsert(ctx, row))
	require.NoError(t, serviceB.Reconcile(ctx))
	assert.True(t, registryB.Supports("shared/model-e"))
	require.NoError(t, storeA.Delete(ctx, "shared"))
	require.NoError(t, serviceB.Reconcile(ctx))
	assert.Nil(t, registryB.ProviderByName("shared"))
	assert.False(t, registryB.Supports("shared/model-e"))
}

func TestCredentialReconcileFailureLoggingUsesLoopContext(t *testing.T) {
	logs := captureSlog(t)
	logCredentialReconcileFailure(context.Background(), context.DeadlineExceeded)
	assert.Contains(t, logs.String(), "provider credential reconciliation failed")
	assert.Contains(t, logs.String(), "context deadline exceeded")

	logs.Reset()
	loopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	logCredentialReconcileFailure(loopCtx, context.Canceled)
	assert.Empty(t, logs.String(), "shutdown cancellation is intentionally quiet")
}

func TestStaleReconcileFailureDoesNotOverwriteLocalSuccess(t *testing.T) {
	store := newFakeCredentialStore()
	factory, _ := newCredentialSyncFactory(t, nil)
	service, err := NewCredentialsService(t.Context(), factory, NewModelRegistry(), store, nil, config.ResilienceConfig{}, nil)
	require.NoError(t, err)
	observedRevision := service.revision("saved")
	row := ManagedProviderCredential{Name: "saved", Type: "test", APIKeys: []string{"current"}, Enabled: true}
	require.NoError(t, service.Upsert(t.Context(), row))

	service.setSkippedIfRevision("saved", observedRevision, errors.New("stale poll build failure"))
	assert.NotContains(t, service.Skipped(), "saved", "an older poll must not replace the newer successful state")
}

func TestCredentialsServiceReconcileRetainsOldInventoryOnFailedRefresh(t *testing.T) {
	ctx := t.Context()
	store := newFakeCredentialStore()
	factory, _ := newCredentialSyncFactory(t, nil)
	registry := NewModelRegistry()
	service, err := NewCredentialsService(ctx, factory, registry, store, nil, config.ResilienceConfig{}, nil)
	require.NoError(t, err)
	row := ManagedProviderCredential{Name: "retained", Type: "test", APIKeys: []string{"good-list"}, Enabled: true}
	require.NoError(t, service.Upsert(ctx, row))
	require.True(t, registry.Supports("retained/old-upstream-model"))

	row.APIKeys = []string{"list-fails"}
	require.NoError(t, store.Upsert(ctx, row))
	require.NoError(t, service.Reconcile(ctx))
	provider, ok := registry.ProviderByName("retained").(*credentialSyncProvider)
	require.True(t, ok)
	assert.Equal(t, "list-fails", provider.cfg.APIKeys[0])
	assert.True(t, registry.Supports("retained/old-upstream-model"))
	assert.Same(t, provider, registry.discoveredByProvider["retained"]["old-upstream-model"].Provider)
}

func TestCredentialsServiceReconcileRejectsStaleBuildAfterSaveOrDelete(t *testing.T) {
	t.Run("save wins", func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		var block atomic.Bool
		factory, _ := newCredentialSyncFactory(t, func(cfg ProviderConfig) {
			if len(cfg.APIKeys) > 0 && cfg.APIKeys[0] == "stale" && block.CompareAndSwap(true, false) {
				close(started)
				<-release
			}
		})
		store := newFakeCredentialStore()
		registry := NewModelRegistry()
		registry.SetConfiguredProviderModelsMode(config.ConfiguredProviderModelsModeAllowlist)
		service, err := NewCredentialsService(t.Context(), factory, registry, store, nil, config.ResilienceConfig{}, nil)
		require.NoError(t, err)
		row := ManagedProviderCredential{Name: "race", Type: "test", APIKeys: []string{"original"}, Models: []string{"old"}, Enabled: true}
		require.NoError(t, store.Upsert(t.Context(), row))
		require.NoError(t, service.Reconcile(t.Context()))

		row.APIKeys = []string{"stale"}
		row.Models = []string{"stale-model"}
		require.NoError(t, store.Upsert(t.Context(), row))
		block.Store(true)
		reconciled := make(chan error, 1)
		go func() { reconciled <- service.Reconcile(context.Background()) }()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			require.FailNow(t, "stale provider build did not start")
		}

		row.APIKeys = []string{"latest"}
		row.Models = []string{"latest-model"}
		require.NoError(t, service.Upsert(t.Context(), row))
		close(release)
		require.NoError(t, <-reconciled)
		provider, ok := registry.ProviderByName("race").(*credentialSyncProvider)
		require.True(t, ok)
		assert.Equal(t, "latest", provider.cfg.APIKeys[0])
		assert.True(t, registry.Supports("race/latest-model"))
		assert.False(t, registry.Supports("race/stale-model"))
	})

	t.Run("delete wins", func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		var block atomic.Bool
		factory, _ := newCredentialSyncFactory(t, func(cfg ProviderConfig) {
			if len(cfg.APIKeys) > 0 && cfg.APIKeys[0] == "stale" && block.CompareAndSwap(true, false) {
				close(started)
				<-release
			}
		})
		store := newFakeCredentialStore()
		registry := NewModelRegistry()
		service, err := NewCredentialsService(t.Context(), factory, registry, store, nil, config.ResilienceConfig{}, nil)
		require.NoError(t, err)
		row := ManagedProviderCredential{Name: "race", Type: "test", APIKeys: []string{"original"}, Models: []string{"old"}, Enabled: true}
		require.NoError(t, store.Upsert(t.Context(), row))
		require.NoError(t, service.Reconcile(t.Context()))
		row.APIKeys = []string{"stale"}
		require.NoError(t, store.Upsert(t.Context(), row))
		block.Store(true)
		reconciled := make(chan error, 1)
		go func() { reconciled <- service.Reconcile(context.Background()) }()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			require.FailNow(t, "stale provider build did not start")
		}
		require.NoError(t, service.Delete(t.Context(), "race"))
		close(release)
		require.NoError(t, <-reconciled)
		assert.Nil(t, registry.ProviderByName("race"))
		assert.False(t, registry.Supports("race/old"))
	})
}

func TestCredentialsServiceReconcileDoesNotOverwriteSecretRotation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var block atomic.Bool
	factory, _ := newCredentialSyncFactory(t, func(cfg ProviderConfig) {
		if block.CompareAndSwap(true, false) && slices.Equal(cfg.Models, []string{"new-model"}) {
			close(started)
			<-release
		}
	})
	store := newFakeCredentialStore()
	registry := NewModelRegistry()
	vault := &secretVault{values: map[string]string{"key": "first"}}
	secrets := config.NewSecrets()
	require.NoError(t, secrets.Register("vault", vault))
	service, err := NewCredentialsService(t.Context(), factory, registry, store, nil, config.ResilienceConfig{}, secrets)
	require.NoError(t, err)
	row := ManagedProviderCredential{Name: "rotate", Type: "test", APIKeys: []string{"${vault:key}"}, Models: []string{"old-model"}, Enabled: true}
	require.NoError(t, store.Upsert(t.Context(), row))
	require.NoError(t, service.Reconcile(t.Context()))

	row.Models = []string{"new-model"}
	require.NoError(t, store.Upsert(t.Context(), row))
	block.Store(true)
	reconciled := make(chan error, 1)
	go func() { reconciled <- service.Reconcile(context.Background()) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "poll build did not start")
	}

	vault.set("key", "rotated")
	recheck, err := secrets.Recheck(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"provider_credentials.rotate.api_keys[0]"}, recheck.Fields())
	require.NoError(t, service.RotateSecrets(t.Context(), recheck.Fields()))
	rotated, ok := registry.ProviderByName("rotate").(*credentialSyncProvider)
	require.True(t, ok)
	assert.Equal(t, "rotated", rotated.cfg.APIKeys[0])

	close(release)
	require.NoError(t, <-reconciled)
	assert.Same(t, rotated, registry.ProviderByName("rotate"))
	assert.Equal(t, "rotated", registry.ProviderByName("rotate").(*credentialSyncProvider).cfg.APIKeys[0])
}

func TestCredentialsResultCloseStopsCredentialSyncBeforeClosingStore(t *testing.T) {
	store := &countedCredentialListStore{CredentialStore: newFakeCredentialStore()}
	factory, _ := newCredentialSyncFactory(t, nil)
	service, err := NewCredentialsService(t.Context(), factory, NewModelRegistry(), store, nil, config.ResilienceConfig{}, nil)
	require.NoError(t, err)
	result := &CredentialsResult{Service: service, Store: store}
	result.StartBackgroundSync(5 * time.Millisecond)
	require.Eventually(t, func() bool { return store.lists.Load() > 1 }, time.Second, time.Millisecond)
	require.NoError(t, result.Close())
	count := store.lists.Load()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, count, store.lists.Load(), "no list operation should continue after Close returns")
	assert.True(t, store.closed.Load(), "the store closes only after the sync loop stops")
}

type countedCredentialListStore struct {
	CredentialStore
	lists  atomic.Int64
	closed atomic.Bool
}

func (s *countedCredentialListStore) List(ctx context.Context) ([]ManagedProviderCredential, error) {
	s.lists.Add(1)
	return s.CredentialStore.List(ctx)
}

func (s *countedCredentialListStore) Close() error {
	s.closed.Store(true)
	return s.CredentialStore.Close()
}

func TestRegistryFullRefreshCannotRestoreReplacedProviderInstance(t *testing.T) {
	registry := NewModelRegistry()
	oldProvider := &credentialSyncProvider{registryMockProvider: &registryMockProvider{
		name:           "old",
		modelsResponse: &core.ModelsResponse{Object: "list", Data: []core.Model{{ID: "model", Object: "model"}}},
	}}
	registry.RegisterProviderWithNameAndType(oldProvider, "managed", "test")
	require.NoError(t, registry.Initialize(t.Context()))

	oldProvider.entered = make(chan struct{}, 1)
	oldProvider.release = make(chan struct{})
	finished := make(chan error, 1)
	go func() { finished <- registry.Initialize(context.Background()) }()
	select {
	case <-oldProvider.entered:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "old provider fetch did not start")
	}

	newProvider := &credentialSyncProvider{registryMockProvider: &registryMockProvider{
		name:           "new",
		modelsResponse: &core.ModelsResponse{Object: "list", Data: []core.Model{{ID: "different", Object: "model"}}},
	}}
	registry.ReplaceProviderWithConfig(newProvider, ProviderConfig{Name: "managed", Type: "test"})
	assert.Same(t, newProvider, registry.discoveredByProvider["managed"]["model"].Provider)
	close(oldProvider.release)
	require.NoError(t, <-finished)
	assert.Same(t, newProvider, registry.discoveredByProvider["managed"]["model"].Provider)
	assert.True(t, registry.Supports("managed/model"))
	assert.False(t, registry.Supports("managed/different"), "the old fetch cannot publish the replaced adapter's inventory")
}
