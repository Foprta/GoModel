package config

import "time"

// ProviderCredentialsConfig controls how often dashboard-managed provider rows
// are reconciled from shared storage.
type ProviderCredentialsConfig struct {
	// RefreshInterval controls replica synchronization. Zero disables periodic
	// synchronization. Default: 30s.
	RefreshInterval time.Duration `yaml:"refresh_interval" env:"PROVIDER_CREDENTIALS_REFRESH_INTERVAL"`
}
