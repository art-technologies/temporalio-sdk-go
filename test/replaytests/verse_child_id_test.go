package replaytests

import (
	"fmt"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"testing"
	"time"
)

func verseChildIDParent(ctx workflow.Context, label string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumAttempts: 2}})
	var got string
	if err := workflow.ExecuteActivity(ctx, "Retry", label).Get(ctx, &got); err != nil {
		return "", err
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	var signal string
	workflow.GetSignalChannel(ctx, "verify").Receive(ctx, &signal)
	if signal != label {
		return "", fmt.Errorf("signal mismatch")
	}
	childctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{Namespace: "benchtest"})
	var result string
	if err := workflow.ExecuteChildWorkflow(childctx, "Child", got).Get(ctx, &result); err != nil {
		return "", err
	}
	return result, nil
}

func TestVerseChildIDReplay(t *testing.T) {
	for _, fixture := range []struct{ name, runID string }{
		{"original", "01a0b3d6-f274-7813-ba7b-672dfa2aa09c"},
		{"reset", "483842f1-85b6-4563-897d-6bbe05eb5416"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			data, err := os.ReadFile("verse-child-" + fixture.name + ".json")
			require.NoError(t, err)
			var history historypb.History
			require.NoError(t, (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, &history))
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(verseChildIDParent, workflow.RegisterOptions{Name: "Parent"})
			require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, &history, worker.ReplayWorkflowHistoryOptions{
				OriginalExecution: workflow.Execution{ID: "maintenance-old-go-sdk-20260918-sdk-reset-fork-1", RunID: fixture.runID},
			}))
		})
	}
}
