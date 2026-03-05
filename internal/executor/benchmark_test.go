package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
	"github.com/Andrew-DataDecode/Granicus/internal/runner"
)

func TestBenchmark_100Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	dir := t.TempDir()
	scriptsDir := filepath.Join(dir, "scripts")
	os.MkdirAll(scriptsDir, 0755)

	// Generate 100 scripts: 10 layers of 10 nodes
	// Layer 0: nodes 0-9 (roots)
	// Layer 1: nodes 10-19 (depend on layer 0)
	// etc.
	configYAML := "pipeline: benchmark\nmax_parallel: 10\nassets:\n"

	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("node_%03d", i)
		source := fmt.Sprintf("scripts/%s.sh", name)

		// Write script
		scriptContent := "#!/bin/bash\necho done\n"
		layer := i / 10
		if layer > 0 {
			// Depend on corresponding node in previous layer
			depName := fmt.Sprintf("node_%03d", (layer-1)*10+(i%10))
			scriptContent = fmt.Sprintf("#!/bin/bash\n# depends_on: %s\necho done\n", depName)
		}

		scriptPath := filepath.Join(scriptsDir, name+".sh")
		if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
			t.Fatal(err)
		}

		configYAML += fmt.Sprintf("  - name: %s\n    type: shell\n    source: %s\n", name, source)
	}

	// Write config
	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(configYAML), 0644)

	// Load and build
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	deps, err := graph.ParseAllDependencies(cfg, dir)
	if err != nil {
		t.Fatalf("deps: %v", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)
	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	shellRunner := runner.NewShellRunner()

	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		r := shellRunner.Run(&runner.Asset{
			Name:   asset.Name,
			Type:   asset.Type,
			Source: asset.Source,
		}, pr, rid)
		return NodeResult{
			AssetName: r.AssetName,
			Status:    r.Status,
			StartTime: r.StartTime,
			EndTime:   r.EndTime,
			Duration:  r.Duration,
			Error:     r.Error,
			ExitCode:  r.ExitCode,
		}
	}

	start := time.Now()
	rr := Execute(g, RunConfig{
		MaxParallel: 10,
		ProjectRoot: dir,
		RunID:       "bench-run",
	}, runnerFunc)
	elapsed := time.Since(start)

	// Verify all succeeded
	if len(rr.Results) != 100 {
		t.Errorf("expected 100 results, got %d", len(rr.Results))
	}
	for _, r := range rr.Results {
		if r.Status != "success" {
			t.Errorf("%s: %s (%s)", r.AssetName, r.Status, r.Error)
		}
	}

	t.Logf("100-node benchmark completed in %v", elapsed)

	if elapsed > 2*time.Second {
		t.Errorf("benchmark took %v, target is < 2 seconds", elapsed)
	}
}
