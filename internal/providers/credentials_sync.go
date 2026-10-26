package providers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// Reconcile applies only credential rows that differ from this service's last
// successfully applied snapshot. A failed list leaves the live registry and
// snapshot untouched; per-row build failures leave that name's working state
// untouched and are retried on the next pass.
func (s *CredentialsService) Reconcile(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := s.store.List(ctx)
	if err != nil {
		return fmt.Errorf("list provider credentials: %w", err)
	}
	refreshNames, reconcileErr := s.reconcileRows(ctx, rows)
	for _, name := range refreshNames {
		s.refreshProviderModels(ctx, name)
	}
	return reconcileErr
}

func (s *CredentialsService) reconcileRows(ctx context.Context, rows []ManagedProviderCredential) ([]string, error) {
	current := make(map[string]ManagedProviderCredential, len(rows))
	for _, row := range rows {
		name := strings.TrimSpace(row.Name)
		if name == "" || name != row.Name {
			return nil, fmt.Errorf("list provider credentials: invalid provider name %q", row.Name)
		}
		if _, exists := current[name]; exists {
			return nil, fmt.Errorf("list provider credentials: duplicate provider name %q", name)
		}
		row.Name = name
		current[name] = cloneCredential(row)
	}

	s.mu.RLock()
	previous := cloneCredentialRows(s.applied)
	s.mu.RUnlock()

	var refreshNames []string
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(current)) {
		row := current[name]
		if s.IsManaged(name) {
			continue
		}
		if applied, ok := previous[name]; ok && sameStoredCredential(&applied, &row) {
			continue
		}

		revision := s.revision(name)
		var built builtCredential
		if row.Enabled {
			var err error
			built, err = s.buildCredential(ctx, row)
			if err != nil {
				s.setSkippedIfRevision(name, revision, err)
				slog.Error("failed to reconcile stored provider credential", "provider", name, "error", err)
				errs = append(errs, fmt.Errorf("provider credential %q: %w", name, err))
				continue
			}
		}

		s.applyMu.Lock()
		if s.revision(name) != revision {
			s.applyMu.Unlock()
			continue
		}
		stored, err := s.storedCredential(ctx, name)
		if err != nil || !sameStoredCredential(stored, &row) {
			s.applyMu.Unlock()
			if err != nil && !errors.Is(err, ErrCredentialNotFound) {
				errs = append(errs, fmt.Errorf("read provider credential %q before reconcile: %w", name, err))
			}
			continue
		}
		if row.Enabled {
			s.install(row, built)
			refreshNames = append(refreshNames, name)
		} else {
			s.remove(name)
			s.setApplied(name, row)
			s.clearSkipped(name)
		}
		s.applyMu.Unlock()
	}

	for _, name := range slices.Sorted(maps.Keys(previous)) {
		if _, exists := current[name]; exists || s.IsManaged(name) {
			continue
		}
		revision := s.revision(name)
		s.applyMu.Lock()
		if s.revision(name) != revision {
			s.applyMu.Unlock()
			continue
		}
		_, err := s.store.Get(ctx, name)
		if errors.Is(err, ErrCredentialNotFound) {
			s.remove(name)
			s.clearSkipped(name)
		} else if err != nil {
			errs = append(errs, fmt.Errorf("check deleted provider credential %q: %w", name, err))
		}
		s.applyMu.Unlock()
	}
	return refreshNames, errors.Join(errs...)
}

func cloneCredentialRows(rows map[string]ManagedProviderCredential) map[string]ManagedProviderCredential {
	copy := make(map[string]ManagedProviderCredential, len(rows))
	for name, row := range rows {
		copy[name] = cloneCredential(row)
	}
	return copy
}

func (s *CredentialsService) setSkippedIfRevision(name string, revision uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revisions[name] != revision {
		return
	}
	if s.skipped == nil {
		s.skipped = make(map[string]error)
	}
	s.skipped[name] = err
}

func (s *CredentialsService) refreshProviderModels(ctx context.Context, name string) {
	if ctx == nil {
		ctx = context.Background()
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := s.registry.RefreshProviderModels(refreshCtx, name); err != nil {
		slog.Warn("provider credential applied but its model inventory refresh failed", "provider", name, "error", err)
	}
}

// StartBackgroundSync periodically reconciles this process with its credential
// store. Stop is blocking and safe to call more than once.
func (s *CredentialsService) StartBackgroundSync(interval time.Duration) func() {
	if interval <= 0 {
		return func() {}
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.syncStop != nil {
		return s.syncStop
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancel()
			<-done
		})
	}
	s.syncStop = stop
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				reconcileCtx, reconcileCancel := context.WithTimeout(loopCtx, 30*time.Second)
				if err := s.Reconcile(reconcileCtx); err != nil {
					logCredentialReconcileFailure(loopCtx, err)
				}
				reconcileCancel()
			}
		}
	}()
	return stop
}

func logCredentialReconcileFailure(loopCtx context.Context, err error) {
	if err != nil && loopCtx.Err() == nil {
		slog.Warn("provider credential reconciliation failed", "error", err)
	}
}

// StopBackgroundSync stops the periodic credential reconciliation loop.
func (s *CredentialsService) StopBackgroundSync() {
	s.syncMu.Lock()
	stop := s.syncStop
	s.syncMu.Unlock()
	if stop != nil {
		stop()
	}
}
