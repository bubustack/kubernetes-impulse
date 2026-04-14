package impulse

import (
	"context"
	"log/slog"
	"os"
	"testing"

	sdkcel "github.com/bubustack/bubu-sdk-go/cel"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	cfgpkg "github.com/bubustack/kubernetes-impulse/pkg/config"
)

const oldObjectFilterCondition = `and (eq .eventType "Modified") ` +
	`(eq .oldObject.status.phase "Pending") ` +
	`(eq .object.status.phase "Failed")`

func TestMatchesFilterSupportsOldObjectAndEventType(t *testing.T) {
	t.Parallel()

	evaluator, err := sdkcel.NewEvaluator(slog.New(slog.NewTextHandler(os.Stdout, nil)), sdkcel.Config{})
	if err != nil {
		t.Fatalf("create evaluator: %v", err)
	}
	t.Cleanup(evaluator.Close)

	imp := &KubernetesImpulse{
		cfg: cfgpkg.Config{
			Watch: &cfgpkg.WatchConfig{
				Filters: &cfgpkg.WatchFilters{
					Condition: oldObjectFilterCondition,
				},
			},
		},
		evaluator: evaluator,
		logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"status": map[string]any{"phase": "Failed"},
		},
	}
	oldObject := map[string]any{
		"status": map[string]any{"phase": "Pending"},
	}

	if !imp.matchesFilter(context.Background(), "Modified", obj, oldObject) {
		t.Fatalf("expected filter to match when eventType and oldObject satisfy condition")
	}
	if imp.matchesFilter(context.Background(), "Added", obj, oldObject) {
		t.Fatalf("expected filter to reject non-matching eventType")
	}
}

func TestGenerateSessionKeyCustomStrategy(t *testing.T) {
	t.Parallel()

	evaluator, err := sdkcel.NewEvaluator(slog.New(slog.NewTextHandler(os.Stdout, nil)), sdkcel.Config{})
	if err != nil {
		t.Fatalf("create evaluator: %v", err)
	}
	t.Cleanup(evaluator.Close)

	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"namespace": "workloads",
				"name":      "api-pod",
			},
		},
	}
	obj.SetNamespace("workloads")
	obj.SetName("api-pod")

	imp := &KubernetesImpulse{
		cfg: cfgpkg.Config{
			SessionKey: &cfgpkg.SessionKeyConfig{
				Strategy:   "custom",
				Expression: `printf "%s/%s:%s" .object.metadata.namespace .object.metadata.name .eventType`,
			},
		},
		evaluator: evaluator,
		logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	got := imp.generateSessionKey(context.Background(), "Deleted", obj, nil)
	want := "workloads/api-pod:Deleted"
	if got != want {
		t.Fatalf("custom session key = %q, want %q", got, want)
	}
}

func TestParseGVRPluralization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		apiVersion string
		kind       string
		want       string
	}{
		{apiVersion: "networking.k8s.io/v1", kind: "Ingress", want: "ingresses"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", want: "networkpolicies"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "Policy", want: "policies"},
	}

	for _, tc := range tests {

		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			got := parseGVR(tc.apiVersion, tc.kind)
			if got.Resource != tc.want {
				t.Fatalf("parseGVR(%q, %q).Resource = %q, want %q", tc.apiVersion, tc.kind, got.Resource, tc.want)
			}
		})
	}
}
