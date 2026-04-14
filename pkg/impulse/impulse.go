// Package impulse implements the Kubernetes impulse logic.
package impulse

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	sdk "github.com/bubustack/bubu-sdk-go"
	sdkcel "github.com/bubustack/bubu-sdk-go/cel"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	sdkk8s "github.com/bubustack/bubu-sdk-go/k8s"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	cfgpkg "github.com/bubustack/kubernetes-impulse/pkg/config"
)

// KubernetesImpulse handles Kubernetes events and submits StoryTrigger requests.
type KubernetesImpulse struct {
	cfg        cfgpkg.Config
	dispatcher *sdk.StoryDispatcher
	logger     *slog.Logger

	// Kubernetes clients
	clientset     *kubernetes.Clientset
	dynamicClient dynamic.Interface
	restMapper    *restmapper.DeferredDiscoveryRESTMapper

	// Debounce tracking
	debounce   map[string]time.Time
	debounceMu sync.Mutex

	// Previous object cache for watch mode.
	oldObjects   map[string]map[string]any
	oldObjectsMu sync.RWMutex

	// Event aggregation
	eventBuffer   map[string][]map[string]any
	eventBufferMu sync.Mutex

	// Condition filter evaluator (SDK templating)
	evaluator *sdkcel.Evaluator

	// Shutdown
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new KubernetesImpulse instance.
func New() *KubernetesImpulse {
	return &KubernetesImpulse{
		logger:      slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		debounce:    make(map[string]time.Time),
		oldObjects:  make(map[string]map[string]any),
		eventBuffer: make(map[string][]map[string]any),
	}
}

// Init initializes the Kubernetes impulse with configuration.
func (i *KubernetesImpulse) Init(ctx context.Context, cfg cfgpkg.Config, secrets *sdkengram.Secrets) error {
	logger := i.loggerWithContext()

	// Verify target story is configured
	targetStory, err := sdk.GetTargetStory()
	if err != nil {
		return fmt.Errorf("failed to get target story from environment: %w", err)
	}
	logger.Info("Target story resolved",
		slog.String("name", targetStory.Name),
		slog.String("namespace", targetStory.Namespace))

	i.cfg = cfg

	// Initialize Kubernetes clients
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	i.clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	i.dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	i.restMapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	// Initialize condition evaluator for filter expressions
	eval, err := sdkcel.NewEvaluator(logger, sdkcel.Config{})
	if err != nil {
		return fmt.Errorf("failed to create template evaluator: %w", err)
	}
	i.evaluator = eval

	// Initialize story dispatcher
	i.dispatcher = sdk.NewStoryDispatcher()

	logger.Info("Kubernetes impulse initialized",
		slog.String("mode", i.cfg.Mode))

	return nil
}

// Run begins watching Kubernetes resources.
func (i *KubernetesImpulse) Run(ctx context.Context, k8sClient *sdkk8s.Client) error {
	logger := i.loggerWithContext()

	ctx, i.cancel = context.WithCancel(ctx)

	// Start health server
	go i.startHealthServer(ctx, logger)

	// Start based on mode
	switch i.cfg.Mode {
	case "watch":
		return i.startWatchMode(ctx, logger)
	case "events":
		return i.startEventsMode(ctx, logger)
	default:
		return fmt.Errorf("unknown mode: %s", i.cfg.Mode)
	}
}

// startHealthServer starts the health check endpoints.
func (i *KubernetesImpulse) startHealthServer(ctx context.Context, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	server := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	logger.Info("Health server starting", slog.String("addr", ":8080"))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Health server error", slog.Any("error", err))
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Watch Mode - Resource state changes
// ═══════════════════════════════════════════════════════════════════════════

func (i *KubernetesImpulse) startWatchMode(ctx context.Context, logger *slog.Logger) error {
	if i.cfg.Watch == nil || len(i.cfg.Watch.Resources) == 0 {
		return fmt.Errorf("watch mode requires at least one resource to watch")
	}

	// Start a watcher for each resource
	for _, res := range i.cfg.Watch.Resources {
		i.wg.Add(1)
		go func(res cfgpkg.ResourceSelector) {
			defer i.wg.Done()
			i.watchResource(ctx, logger, res)
		}(res)
	}

	logger.Info("Watch mode started",
		slog.Int("resourceCount", len(i.cfg.Watch.Resources)))

	// Block until context is cancelled
	<-ctx.Done()
	return nil
}

func (i *KubernetesImpulse) watchResource(ctx context.Context, logger *slog.Logger, res cfgpkg.ResourceSelector) {
	gvr := i.resolveGVR(logger, res.APIVersion, res.Kind)
	logger = logger.With(
		slog.String("apiVersion", res.APIVersion),
		slog.String("kind", res.Kind),
		slog.String("namespace", res.Namespace))

	logger.Info("Starting resource watcher")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Create watch
		var watcher watch.Interface
		var err error

		listOpts := metav1.ListOptions{
			LabelSelector: res.LabelSelector,
			FieldSelector: res.FieldSelector,
		}

		if res.Namespace != "" {
			watcher, err = i.dynamicClient.Resource(gvr).Namespace(res.Namespace).Watch(ctx, listOpts)
		} else {
			watcher, err = i.dynamicClient.Resource(gvr).Watch(ctx, listOpts)
		}

		if err != nil {
			logger.Error("Failed to create watcher", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}

		// Process events
		i.processWatchEvents(ctx, logger, watcher)
		watcher.Stop()

		// Brief pause before reconnecting
		time.Sleep(time.Second)
	}
}

func (i *KubernetesImpulse) processWatchEvents(
	ctx context.Context,
	logger *slog.Logger,
	watcher watch.Interface,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				logger.Debug("Watch channel closed, reconnecting")
				return
			}

			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				logger.Warn("Unexpected object type")
				continue
			}
			eventType := string(event.Type)

			// Skip BubuStack's own workload pods to prevent infinite feedback loops
			if isBubuStackWorkload(obj) {
				continue
			}

			resourceKey := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
			oldObject := i.getCachedOldObject(resourceKey)

			if !i.shouldTrigger(eventType) {
				i.updateCachedObject(resourceKey, eventType, obj)
				continue
			}

			// Evaluate filter condition BEFORE debounce — debounce should only
			// apply to events that pass the filter, otherwise the first non-matching
			// event (e.g. phase=Pending) would suppress the matching one (phase=Failed).
			if !i.matchesFilter(ctx, eventType, obj, oldObject) {
				i.updateCachedObject(resourceKey, eventType, obj)
				continue
			}

			// Check debounce (only for events that pass the filter)
			if i.isDebounced(resourceKey) {
				logger.Debug("Event debounced", slog.String("resource", resourceKey))
				i.updateCachedObject(resourceKey, eventType, obj)
				continue
			}
			inputs := map[string]any{
				"mode":      "watch",
				"eventType": eventType,
				"object":    obj.Object,
			}
			if i.shouldIncludeOldObject() && oldObject != nil {
				inputs["oldObject"] = oldObject
			}
			// Merge static inputs (e.g. discordChannelId) from config
			for k, v := range i.cfg.StaticInputs {
				inputs[k] = v
			}

			// Generate session key
			sessionKey := i.generateSessionKey(ctx, eventType, obj, oldObject)

			logger.Info("Triggering story",
				slog.String("eventType", eventType),
				slog.String("resource", resourceKey),
				slog.String("sessionKey", sessionKey))

			// Trigger story
			go i.triggerStory(ctx, logger, sessionKey, inputs)
			i.updateCachedObject(resourceKey, eventType, obj)
		}
	}
}

func (i *KubernetesImpulse) shouldTrigger(eventType string) bool {
	if i.cfg.Watch == nil {
		return false
	}
	for _, t := range i.cfg.Watch.Triggers {
		if strings.EqualFold(t, eventType) {
			return true
		}
	}
	return false
}

// isBubuStackWorkload returns true if the object is a BubuStack-managed workload pod
// (e.g., StepRun analyzer pods). These must be excluded to prevent infinite feedback loops.
func isBubuStackWorkload(obj *unstructured.Unstructured) bool {
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	// Check both label conventions used by bobrapet
	_, hasStepRun := labels["bubustack.io/steprun"]
	_, hasEngram := labels["bubustack.io/engram"]
	_, hasImpulse := labels["bubustack.io/impulse"]
	managedBy := labels["app.kubernetes.io/managed-by"]
	return hasStepRun || hasEngram || hasImpulse || managedBy == "bobrapet-operator"
}

// matchesFilter evaluates the watch filter condition against the object.
// Uses core's Go template engine — the condition expression has access to the full object.
func (i *KubernetesImpulse) matchesFilter(
	ctx context.Context,
	eventType string,
	obj *unstructured.Unstructured,
	oldObject map[string]any,
) bool {
	if i.cfg.Watch == nil || i.cfg.Watch.Filters == nil || strings.TrimSpace(i.cfg.Watch.Filters.Condition) == "" {
		return true
	}
	vars := map[string]any{
		"object":    obj.Object,
		"eventType": eventType,
		"oldObject": oldObject,
	}
	// Auto-wrap bare expressions in {{ }} for the template engine
	condition := strings.TrimSpace(i.cfg.Watch.Filters.Condition)
	if !strings.Contains(condition, "{{") {
		condition = "{{ " + condition + " }}"
	}
	matched, err := i.evaluator.EvaluateCondition(ctx, condition, vars)

	if err != nil {
		i.loggerWithContext().Warn("Filter condition evaluation failed, skipping event",
			slog.String("error", err.Error()),
			slog.String("resource", fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())))
		return false
	}
	return matched
}

func (i *KubernetesImpulse) isDebounced(key string) bool {
	if i.cfg.Watch == nil || i.cfg.Watch.Filters == nil || i.cfg.Watch.Filters.DebounceSeconds <= 0 {
		return false
	}

	i.debounceMu.Lock()
	defer i.debounceMu.Unlock()

	debounceWindow := time.Duration(i.cfg.Watch.Filters.DebounceSeconds) * time.Second
	lastSeen, exists := i.debounce[key]
	now := time.Now()

	if exists && now.Sub(lastSeen) < debounceWindow {
		return true
	}

	i.debounce[key] = now
	return false
}

// ═══════════════════════════════════════════════════════════════════════════
// Events Mode - Kubernetes Event objects
// ═══════════════════════════════════════════════════════════════════════════

func (i *KubernetesImpulse) startEventsMode(ctx context.Context, logger *slog.Logger) error {
	if i.cfg.Events == nil {
		return fmt.Errorf("events mode requires events configuration")
	}

	// If aggregation is enabled, start the flush goroutine
	if i.cfg.Events.Aggregation != nil && i.cfg.Events.Aggregation.Enabled {
		i.wg.Add(1)
		go func() {
			defer i.wg.Done()
			i.flushAggregatedEvents(ctx, logger)
		}()
	}

	// Watch events in specified namespaces (or all)
	namespaces := i.cfg.Events.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""} // Empty string means all namespaces
	}

	for _, ns := range namespaces {
		i.wg.Add(1)
		go func(ns string) {
			defer i.wg.Done()
			i.watchEvents(ctx, logger, ns)
		}(ns)
	}

	logger.Info("Events mode started",
		slog.Any("namespaces", namespaces),
		slog.Any("types", i.cfg.Events.Types),
		slog.Any("reasons", i.cfg.Events.Reasons))

	<-ctx.Done()
	return nil
}

func (i *KubernetesImpulse) watchEvents(ctx context.Context, logger *slog.Logger, namespace string) {
	logger = logger.With(slog.String("namespace", namespace))
	logger.Info("Starting events watcher")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var watcher watch.Interface
		var err error

		if namespace != "" {
			watcher, err = i.clientset.CoreV1().Events(namespace).Watch(ctx, metav1.ListOptions{})
		} else {
			watcher, err = i.clientset.CoreV1().Events("").Watch(ctx, metav1.ListOptions{})
		}

		if err != nil {
			logger.Error("Failed to create events watcher", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}

		i.processEventObjects(ctx, logger, watcher)
		watcher.Stop()

		time.Sleep(time.Second)
	}
}

func (i *KubernetesImpulse) processEventObjects(ctx context.Context, logger *slog.Logger, watcher watch.Interface) {
	for {
		select {
		case <-ctx.Done():
			return
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				return
			}

			if watchEvent.Type != watch.Added && watchEvent.Type != watch.Modified {
				continue
			}

			event, ok := watchEvent.Object.(*corev1.Event)
			if !ok {
				continue
			}

			// Apply filters
			if !i.matchesEventFilters(event) {
				continue
			}

			eventData := map[string]any{
				"type":           event.Type,
				"reason":         event.Reason,
				"message":        event.Message,
				"count":          event.Count,
				"firstTimestamp": event.FirstTimestamp.Time,
				"lastTimestamp":  event.LastTimestamp.Time,
				"involvedObject": map[string]any{
					"apiVersion": event.InvolvedObject.APIVersion,
					"kind":       event.InvolvedObject.Kind,
					"name":       event.InvolvedObject.Name,
					"namespace":  event.InvolvedObject.Namespace,
					"uid":        string(event.InvolvedObject.UID),
				},
			}

			// Handle aggregation
			if i.cfg.Events.Aggregation != nil && i.cfg.Events.Aggregation.Enabled {
				i.addToEventBuffer(event, eventData)
			} else {
				// Immediate trigger
				inputs := map[string]any{
					"mode":   "events",
					"events": []map[string]any{eventData},
				}
				sessionKey := fmt.Sprintf("%s/%s/%s",
					event.InvolvedObject.Namespace,
					event.InvolvedObject.Name,
					event.Reason)

				logger.Info("Triggering story for event",
					slog.String("reason", event.Reason),
					slog.String("involvedObject", event.InvolvedObject.Name))

				go i.triggerStory(ctx, logger, sessionKey, inputs)
			}
		}
	}
}

func (i *KubernetesImpulse) matchesEventFilters(event *corev1.Event) bool {
	cfg := i.cfg.Events

	// Filter by type
	if len(cfg.Types) > 0 {
		found := false
		for _, t := range cfg.Types {
			if t == event.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by reason
	if len(cfg.Reasons) > 0 {
		found := false
		for _, r := range cfg.Reasons {
			if r == event.Reason {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Filter by involved object kind
	if len(cfg.InvolvedObjectKinds) > 0 {
		found := false
		for _, k := range cfg.InvolvedObjectKinds {
			if k == event.InvolvedObject.Kind {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func (i *KubernetesImpulse) addToEventBuffer(event *corev1.Event, eventData map[string]any) {
	i.eventBufferMu.Lock()
	defer i.eventBufferMu.Unlock()

	// Key by involved object
	key := fmt.Sprintf("%s/%s", event.InvolvedObject.Namespace, event.InvolvedObject.Name)
	i.eventBuffer[key] = append(i.eventBuffer[key], eventData)
}

func (i *KubernetesImpulse) flushAggregatedEvents(ctx context.Context, logger *slog.Logger) {
	windowSeconds := 60
	minCount := 1

	if i.cfg.Events.Aggregation != nil {
		if i.cfg.Events.Aggregation.WindowSeconds > 0 {
			windowSeconds = i.cfg.Events.Aggregation.WindowSeconds
		}
		if i.cfg.Events.Aggregation.MinCount > 0 {
			minCount = i.cfg.Events.Aggregation.MinCount
		}
	}

	ticker := time.NewTicker(time.Duration(windowSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.eventBufferMu.Lock()
			for key, events := range i.eventBuffer {
				if len(events) >= minCount {
					inputs := map[string]any{
						"mode":   "events",
						"events": events,
					}

					logger.Info("Flushing aggregated events",
						slog.String("key", key),
						slog.Int("count", len(events)))

					go i.triggerStory(ctx, logger, key, inputs)
				}
				delete(i.eventBuffer, key)
			}
			i.eventBufferMu.Unlock()
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Common utilities
// ═══════════════════════════════════════════════════════════════════════════

func (i *KubernetesImpulse) generateSessionKey(
	ctx context.Context,
	eventType string,
	obj *unstructured.Unstructured,
	oldObject map[string]any,
) string {
	strategy := "auto"
	if i.cfg.SessionKey != nil {
		strategy = i.cfg.SessionKey.Strategy
	}

	switch strategy {
	case "unique":
		return fmt.Sprintf("%s-%s-%d", obj.GetNamespace(), obj.GetName(), time.Now().UnixNano())
	case "custom":
		if i.cfg.SessionKey != nil && i.evaluator != nil {
			expr := strings.TrimSpace(i.cfg.SessionKey.Expression)
			if expr != "" {
				vars := map[string]any{
					"eventType": eventType,
					"object":    obj.Object,
					"oldObject": oldObject,
				}
				resolved, err := i.evaluator.EvaluateExpression(ctx, expr, vars)
				if err != nil {
					i.loggerWithContext().Warn("Failed to evaluate custom session key expression; falling back to auto strategy",
						slog.String("expression", expr),
						slog.Any("error", err))
				} else {
					key := strings.TrimSpace(fmt.Sprint(resolved))
					if key != "" {
						return key
					}
				}
			}
		}
		fallthrough
	case "auto":
		fallthrough
	default:
		return fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	}
}

func (i *KubernetesImpulse) triggerStory(
	ctx context.Context,
	logger *slog.Logger,
	sessionKey string,
	inputs map[string]any,
) {
	req := sdk.StoryTriggerRequest{
		Key:       sessionKey,
		StoryName: "", // SDK resolves from Impulse.spec.storyRef
		Inputs:    inputs,
	}

	result, err := i.dispatcher.Trigger(ctx, req)
	if err != nil {
		logger.Error("Failed to trigger story",
			slog.String("sessionKey", sessionKey),
			slog.Any("error", err))
		return
	}
	storyRunName := ""
	if result != nil && result.StoryRun != nil {
		storyRunName = strings.TrimSpace(result.StoryRun.Name)
	}
	if storyRunName == "" && result != nil && result.Session != nil {
		storyRunName = strings.TrimSpace(result.Session.StoryRun)
	}
	if storyRunName == "" {
		logger.Error("Trigger returned no StoryRun identity",
			slog.String("sessionKey", sessionKey))
		return
	}

	logger.Info("StoryRun created",
		slog.String("storyRun", storyRunName),
		slog.String("sessionKey", sessionKey))
}

func (i *KubernetesImpulse) loggerWithContext() *slog.Logger {
	return i.logger.With(slog.String("component", "kubernetes-impulse"))
}

func (i *KubernetesImpulse) shouldIncludeOldObject() bool {
	return i.cfg.Watch != nil && i.cfg.Watch.Filters != nil && i.cfg.Watch.Filters.IncludeOldObject
}

func (i *KubernetesImpulse) getCachedOldObject(resourceKey string) map[string]any {
	if !i.shouldIncludeOldObject() {
		return nil
	}
	i.oldObjectsMu.RLock()
	defer i.oldObjectsMu.RUnlock()
	if obj, ok := i.oldObjects[resourceKey]; ok {
		return k8sruntime.DeepCopyJSON(obj)
	}
	return nil
}

func (i *KubernetesImpulse) updateCachedObject(resourceKey, eventType string, obj *unstructured.Unstructured) {
	if !i.shouldIncludeOldObject() || obj == nil {
		return
	}
	i.oldObjectsMu.Lock()
	defer i.oldObjectsMu.Unlock()
	switch {
	case strings.EqualFold(eventType, string(watch.Deleted)):
		delete(i.oldObjects, resourceKey)
	default:
		i.oldObjects[resourceKey] = k8sruntime.DeepCopyJSON(obj.Object)
	}
}

func (i *KubernetesImpulse) resolveGVR(
	logger *slog.Logger,
	apiVersion string,
	kind string,
) schema.GroupVersionResource {
	fallback := parseGVR(apiVersion, kind)
	if i.restMapper == nil {
		return fallback
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		if logger != nil {
			logger.Warn("Failed to parse apiVersion, using fallback resource mapping",
				slog.String("apiVersion", apiVersion),
				slog.String("kind", kind),
				slog.Any("error", err))
		}
		return fallback
	}
	mapping, err := i.restMapper.RESTMapping(schema.GroupKind{
		Group: gv.Group,
		Kind:  kind,
	}, gv.Version)
	if err != nil {
		if logger != nil && !apierrors.IsNotFound(err) {
			logger.Warn("Failed to resolve resource mapping from discovery, using fallback pluralization",
				slog.String("apiVersion", apiVersion),
				slog.String("kind", kind),
				slog.Any("error", err))
		}
		return fallback
	}
	return mapping.Resource
}

// parseGVR parses an API version and kind into a GroupVersionResource.
func parseGVR(apiVersion, kind string) schema.GroupVersionResource {
	gv, _ := schema.ParseGroupVersion(apiVersion)

	lowerKind := strings.ToLower(kind)
	resource := lowerKind + "s"
	switch lowerKind {
	case "ingress":
		resource = "ingresses"
	case "networkpolicy":
		resource = "networkpolicies"
	case "endpoints":
		resource = "endpoints"
	default:
		switch {
		case strings.HasSuffix(lowerKind, "s"),
			strings.HasSuffix(lowerKind, "x"),
			strings.HasSuffix(lowerKind, "z"),
			strings.HasSuffix(lowerKind, "ch"),
			strings.HasSuffix(lowerKind, "sh"):
			resource = lowerKind + "es"
		case strings.HasSuffix(lowerKind, "y") && len(lowerKind) > 1:
			before := lowerKind[len(lowerKind)-2]
			if !strings.ContainsRune("aeiou", rune(before)) {
				resource = lowerKind[:len(lowerKind)-1] + "ies"
			}
		}
	}

	return schema.GroupVersionResource{
		Group:    gv.Group,
		Version:  gv.Version,
		Resource: resource,
	}
}
