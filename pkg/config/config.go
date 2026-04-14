// Package config defines configuration structures for the Kubernetes impulse.
package config

// Config holds the Kubernetes impulse configuration.
type Config struct {
	Mode         string            `json:"mode" mapstructure:"mode"`
	Watch        *WatchConfig      `json:"watch,omitempty" mapstructure:"watch"`
	Events       *EventsConfig     `json:"events,omitempty" mapstructure:"events"`
	SessionKey   *SessionKeyConfig `json:"sessionKey,omitempty" mapstructure:"sessionKey"`
	StaticInputs map[string]any    `json:"staticInputs,omitempty" mapstructure:"staticInputs"`
}

// WatchConfig configures resource watching.
type WatchConfig struct {
	Resources []ResourceSelector `json:"resources" mapstructure:"resources"`
	Triggers  []string           `json:"triggers" mapstructure:"triggers"`
	Filters   *WatchFilters      `json:"filters,omitempty" mapstructure:"filters"`
}

// ResourceSelector defines which resources to watch.
type ResourceSelector struct {
	APIVersion    string `json:"apiVersion" mapstructure:"apiVersion"`
	Kind          string `json:"kind" mapstructure:"kind"`
	Namespace     string `json:"namespace,omitempty" mapstructure:"namespace"`
	LabelSelector string `json:"labelSelector,omitempty" mapstructure:"labelSelector"`
	FieldSelector string `json:"fieldSelector,omitempty" mapstructure:"fieldSelector"`
}

// WatchFilters defines additional filtering for watch mode.
type WatchFilters struct {
	Condition        string `json:"condition,omitempty" mapstructure:"condition"`
	DebounceSeconds  int    `json:"debounceSeconds,omitempty" mapstructure:"debounceSeconds"`
	IncludeOldObject bool   `json:"includeOldObject" mapstructure:"includeOldObject"`
}

// EventsConfig configures Kubernetes Event watching.
type EventsConfig struct {
	Namespaces          []string           `json:"namespaces,omitempty" mapstructure:"namespaces"`
	Types               []string           `json:"types,omitempty" mapstructure:"types"`
	Reasons             []string           `json:"reasons,omitempty" mapstructure:"reasons"`
	InvolvedObjectKinds []string           `json:"involvedObjectKinds,omitempty" mapstructure:"involvedObjectKinds"`
	Aggregation         *AggregationConfig `json:"aggregation,omitempty" mapstructure:"aggregation"`
}

// AggregationConfig configures event aggregation.
type AggregationConfig struct {
	Enabled       bool `json:"enabled" mapstructure:"enabled"`
	WindowSeconds int  `json:"windowSeconds" mapstructure:"windowSeconds"`
	MinCount      int  `json:"minCount" mapstructure:"minCount"`
}

// SessionKeyConfig configures session key generation.
type SessionKeyConfig struct {
	Strategy   string `json:"strategy" mapstructure:"strategy"`
	Expression string `json:"expression,omitempty" mapstructure:"expression"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Mode: "watch",
		Watch: &WatchConfig{
			Triggers: []string{"Added", "Modified", "Deleted"},
			Filters: &WatchFilters{
				IncludeOldObject: true,
			},
		},
		Events: &EventsConfig{
			Types: []string{"Warning"},
			Aggregation: &AggregationConfig{
				WindowSeconds: 60,
				MinCount:      1,
			},
		},
		SessionKey: &SessionKeyConfig{
			Strategy: "auto",
		},
	}
}
