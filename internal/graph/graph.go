package graph

import (
	"fmt"
	"strings"
	"time"
)

type Asset struct {
	Name                  string
	Type                  string
	Source                string
	DependsOn             []string
	DependedOnBy          []string
	DestinationConnection string
	SourceConnection      string
	TimeColumn            string
	IntervalUnit          string
	Lookback              int
	StartDate             string
	BatchSize             int
	SourceAsset           string // back-reference for multi-output nodes
	IntervalStart         string // set by executor at runtime
	IntervalEnd           string // set by executor at runtime
	TestStart             string // set by executor for @test_start substitution
	TestEnd               string // set by executor for @test_end substitution
	Layer                 string
	Grain                 string
	DefaultChecks         *bool
	InlineSQL             string
	Blocking              bool
	Timeout               time.Duration
}

type Graph struct {
	Assets    map[string]*Asset
	RootNodes []string
}

func BuildGraph(assets []AssetInput, deps map[string][]string) (*Graph, error) {
	g := &Graph{
		Assets: make(map[string]*Asset),
	}

	for _, a := range assets {
		if _, exists := g.Assets[a.Name]; exists {
			return nil, fmt.Errorf("duplicate asset name: %q", a.Name)
		}
		g.Assets[a.Name] = &Asset{
			Name:                  a.Name,
			Type:                  a.Type,
			Source:                a.Source,
			DestinationConnection: a.DestinationConnection,
			SourceConnection:      a.SourceConnection,
			TimeColumn:            a.TimeColumn,
			IntervalUnit:          a.IntervalUnit,
			Lookback:              a.Lookback,
			StartDate:             a.StartDate,
			BatchSize:             a.BatchSize,
			SourceAsset:           a.SourceAsset,
			Layer:                 a.Layer,
			Grain:                 a.Grain,
			DefaultChecks:         a.DefaultChecks,
			InlineSQL:             a.InlineSQL,
			Blocking:              a.Blocking,
			Timeout:               a.Timeout,
		}
	}

	for name, depList := range deps {
		asset, ok := g.Assets[name]
		if !ok {
			continue
		}
		for _, dep := range depList {
			if _, ok := g.Assets[dep]; !ok {
				return nil, fmt.Errorf("asset %q depends on %q which does not exist", name, dep)
			}
			asset.DependsOn = append(asset.DependsOn, dep)
			g.Assets[dep].DependedOnBy = append(g.Assets[dep].DependedOnBy, name)
		}
	}

	for name, asset := range g.Assets {
		if len(asset.DependsOn) == 0 {
			g.RootNodes = append(g.RootNodes, name)
		}
	}

	if err := g.detectCycles(); err != nil {
		return nil, err
	}

	return g, nil
}

type AssetInput struct {
	Name                  string
	Type                  string
	Source                string
	DestinationConnection string
	SourceConnection      string
	TimeColumn            string
	IntervalUnit          string
	Lookback              int
	StartDate             string
	BatchSize             int
	SourceAsset           string
	Layer                 string
	Grain                 string
	DefaultChecks         *bool
	InlineSQL             string
	Blocking              bool
	Timeout               time.Duration
}

const AssetTypeSource = "source"

const (
	white = 0
	gray  = 1
	black = 2
)

func (g *Graph) detectCycles() error {
	color := make(map[string]int)
	parent := make(map[string]string)

	for name := range g.Assets {
		color[name] = white
	}

	for name := range g.Assets {
		if color[name] == white {
			if cycle := g.dfs(name, color, parent); cycle != "" {
				return fmt.Errorf("cycle detected: %s", cycle)
			}
		}
	}
	return nil
}

func (g *Graph) dfs(node string, color map[string]int, parent map[string]string) string {
	color[node] = gray

	for _, dep := range g.Assets[node].DependsOn {
		if color[dep] == gray {
			// Build cycle path
			path := []string{dep, node}
			cur := node
			for cur != dep {
				cur = parent[cur]
				path = append(path, cur)
			}
			// Reverse and format
			for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
				path[i], path[j] = path[j], path[i]
			}
			return strings.Join(path, " -> ")
		}
		if color[dep] == white {
			parent[dep] = node
			if cycle := g.dfs(dep, color, parent); cycle != "" {
				return cycle
			}
		}
	}

	color[node] = black
	return ""
}

func (g *Graph) TopologicalSort() []string {
	inDegree := make(map[string]int)
	for name := range g.Assets {
		inDegree[name] = 0
	}
	for _, asset := range g.Assets {
		for _, dep := range asset.DependsOn {
			inDegree[asset.Name]++
			_ = dep
		}
	}

	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var result []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)

		for _, downstream := range g.Assets[node].DependedOnBy {
			inDegree[downstream]--
			if inDegree[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
	}

	return result
}

func (g *Graph) Descendants(name string) []string {
	visited := make(map[string]bool)
	g.collectDescendants(name, visited)
	delete(visited, name)

	var result []string
	for n := range visited {
		result = append(result, n)
	}
	return result
}

func (g *Graph) collectDescendants(name string, visited map[string]bool) {
	if visited[name] {
		return
	}
	visited[name] = true
	for _, child := range g.Assets[name].DependedOnBy {
		g.collectDescendants(child, visited)
	}
}

func (g *Graph) Subgraph(targets []string) []string {
	visited := make(map[string]bool)
	for _, t := range targets {
		g.collectAncestors(t, visited)
	}

	var result []string
	for n := range visited {
		result = append(result, n)
	}
	return result
}

func (g *Graph) collectAncestors(name string, visited map[string]bool) {
	if visited[name] {
		return
	}
	visited[name] = true
	for _, dep := range g.Assets[name].DependsOn {
		g.collectAncestors(dep, visited)
	}
}

func (g *Graph) DownstreamSubgraph(targets []string) []string {
	visited := make(map[string]bool)
	for _, t := range targets {
		g.collectDescendants(t, visited)
	}

	var result []string
	for n := range visited {
		result = append(result, n)
	}
	return result
}
