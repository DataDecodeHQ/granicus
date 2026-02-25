package executor

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/graph"
)

func successRunner(delay time.Duration) RunnerFunc {
	return func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		time.Sleep(delay)
		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			StartTime: time.Now().Add(-delay),
			EndTime:   time.Now(),
			Duration:  delay,
			ExitCode:  0,
		}
	}
}

func failRunner(asset *graph.Asset, projectRoot string, runID string) NodeResult {
	return NodeResult{
		AssetName: asset.Name,
		Status:    "failed",
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Error:     "exit status 1",
		ExitCode:  1,
	}
}

func buildTestGraph(t *testing.T, assets []graph.AssetInput, deps map[string][]string) *graph.Graph {
	t.Helper()
	g, err := graph.BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func resultMap(rr *RunResult) map[string]string {
	m := make(map[string]string)
	for _, r := range rr.Results {
		m[r.AssetName] = r.Status
	}
	return m
}

func TestExecute_LinearChain(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
		},
		map[string][]string{"B": {"A"}, "C": {"B"}},
	)

	var order []string
	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		order = append(order, asset.Name)
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["A"] != "success" || rm["B"] != "success" || rm["C"] != "success" {
		t.Errorf("results: %v", rm)
	}
	if len(order) != 3 || order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Errorf("expected [A B C], got %v", order)
	}
}

func TestExecute_ParallelRoots(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
		},
		nil,
	)

	var running int32
	var maxConcurrent int32
	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		cur := atomic.AddInt32(&running, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 10}, runner)
	if len(rr.Results) != 3 {
		t.Errorf("expected 3 results, got %d", len(rr.Results))
	}
	if atomic.LoadInt32(&maxConcurrent) < 2 {
		t.Error("expected at least 2 concurrent executions for parallel roots")
	}
}

func TestExecute_DiamondDependency(t *testing.T) {
	// A -> B, A -> C, B -> D, C -> D
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
			{Name: "D", Type: "shell", Source: "d.sh"},
		},
		map[string][]string{"B": {"A"}, "C": {"A"}, "D": {"B", "C"}},
	)

	rr := Execute(g, RunConfig{MaxParallel: 10}, successRunner(0))
	rm := resultMap(rr)
	if rm["D"] != "success" {
		t.Errorf("D should succeed, got %s", rm["D"])
	}
}

func TestExecute_FailurePropagation(t *testing.T) {
	// A (root), B (root, fails), C depends on B, D depends on A
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
			{Name: "D", Type: "shell", Source: "d.sh"},
		},
		map[string][]string{"C": {"B"}, "D": {"A"}},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "B" {
			return failRunner(asset, pr, rid)
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 10}, runner)
	rm := resultMap(rr)

	if rm["A"] != "success" {
		t.Errorf("A should succeed, got %s", rm["A"])
	}
	if rm["B"] != "failed" {
		t.Errorf("B should fail, got %s", rm["B"])
	}
	if rm["C"] != "skipped" {
		t.Errorf("C should be skipped, got %s", rm["C"])
	}
	if rm["D"] != "success" {
		t.Errorf("D should succeed, got %s", rm["D"])
	}
}

func TestExecute_MaxParallel(t *testing.T) {
	// 5 independent nodes, max_parallel=2, each takes 100ms
	var assets []graph.AssetInput
	for i := 0; i < 5; i++ {
		assets = append(assets, graph.AssetInput{Name: fmt.Sprintf("n%d", i), Type: "shell", Source: fmt.Sprintf("n%d.sh", i)})
	}

	g := buildTestGraph(t, assets, nil)

	start := time.Now()
	rr := Execute(g, RunConfig{MaxParallel: 2}, successRunner(100*time.Millisecond))
	elapsed := time.Since(start)

	if len(rr.Results) != 5 {
		t.Errorf("expected 5 results, got %d", len(rr.Results))
	}
	// With max_parallel=2 and 5 nodes at 100ms each: 3 batches = ~300ms
	if elapsed < 250*time.Millisecond {
		t.Errorf("too fast (%v), semaphore may not be working", elapsed)
	}
	if elapsed > 1*time.Second {
		t.Errorf("too slow (%v), parallelism may be broken", elapsed)
	}
}

func TestExecute_AllFail(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "R1", Type: "shell", Source: "r1.sh"},
			{Name: "R2", Type: "shell", Source: "r2.sh"},
			{Name: "C1", Type: "shell", Source: "c1.sh"},
		},
		map[string][]string{"C1": {"R1", "R2"}},
	)

	rr := Execute(g, RunConfig{MaxParallel: 10}, failRunner)
	rm := resultMap(rr)

	if rm["R1"] != "failed" || rm["R2"] != "failed" {
		t.Errorf("roots should fail: %v", rm)
	}
	if rm["C1"] != "skipped" {
		t.Errorf("C1 should be skipped, got %s", rm["C1"])
	}
}

func TestExecute_EmptyGraph(t *testing.T) {
	g := buildTestGraph(t, nil, nil)
	rr := Execute(g, RunConfig{MaxParallel: 10}, successRunner(0))
	if len(rr.Results) != 0 {
		t.Errorf("expected 0 results, got %d", len(rr.Results))
	}
}

func TestExecute_SingleNode(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{{Name: "only", Type: "shell", Source: "only.sh"}},
		nil,
	)
	rr := Execute(g, RunConfig{MaxParallel: 10}, successRunner(0))
	rm := resultMap(rr)
	if rm["only"] != "success" {
		t.Errorf("expected success, got %s", rm["only"])
	}
}

func TestExecute_SubgraphExecution(t *testing.T) {
	// A -> B -> C, A -> D, E (independent)
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
			{Name: "D", Type: "shell", Source: "d.sh"},
			{Name: "E", Type: "shell", Source: "e.sh"},
		},
		map[string][]string{"B": {"A"}, "C": {"B"}, "D": {"A"}},
	)

	var executed []string
	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		executed = append(executed, asset.Name)
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	Execute(g, RunConfig{MaxParallel: 1, Assets: []string{"C"}}, runner)

	// Should run A, B, C only (not D or E)
	has := make(map[string]bool)
	for _, e := range executed {
		has[e] = true
	}
	if !has["A"] || !has["B"] || !has["C"] {
		t.Errorf("should have run A,B,C: %v", executed)
	}
	if has["D"] || has["E"] {
		t.Errorf("should not have run D or E: %v", executed)
	}
}

func TestExecute_LargeGraph(t *testing.T) {
	var assets []graph.AssetInput
	deps := make(map[string][]string)

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("node_%03d", i)
		assets = append(assets, graph.AssetInput{Name: name, Type: "shell", Source: name + ".sh"})
		if i > 0 {
			prev := fmt.Sprintf("node_%03d", i-1)
			deps[name] = []string{prev}
		}
	}

	g := buildTestGraph(t, assets, deps)
	rr := Execute(g, RunConfig{MaxParallel: 10}, successRunner(0))

	if len(rr.Results) != 100 {
		t.Errorf("expected 100 results, got %d", len(rr.Results))
	}

	for _, r := range rr.Results {
		if r.Status != "success" {
			t.Errorf("%s: %s", r.AssetName, r.Status)
		}
	}
}
