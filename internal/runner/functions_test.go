package runner

import (
	"os"
	"path/filepath"
	"testing"
	"text/template"
)

func TestBuiltinFuncMap_CastToCurrency(t *testing.T) {
	fm := BuiltinFuncMap()
	fn, ok := fm["cast_to_currency"]
	if !ok {
		t.Fatal("cast_to_currency not found in builtin FuncMap")
	}
	result := fn.(func(string) string)("revenue")
	expected := "CAST(ROUND(revenue, 2) AS NUMERIC)"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestLoadFunctions_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	fm, err := LoadFunctions(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(fm) != 0 {
		t.Errorf("expected empty FuncMap, got %d entries", len(fm))
	}
}

func TestLoadFunctions_NonexistentDir(t *testing.T) {
	fm, err := LoadFunctions("/nonexistent/path")
	if err != nil {
		t.Fatal(err)
	}
	if len(fm) != 0 {
		t.Errorf("expected empty FuncMap, got %d entries", len(fm))
	}
}

func TestLoadFunctions_EmptyString(t *testing.T) {
	fm, err := LoadFunctions("")
	if err != nil {
		t.Fatal(err)
	}
	if len(fm) != 0 {
		t.Errorf("expected empty FuncMap, got %d entries", len(fm))
	}
}

func TestLoadFunctions_SQLFiles(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "timestamp_to_utc.sql"), []byte("TIMESTAMP_TRUNC($1, DAY, 'UTC')"), 0644)
	os.WriteFile(filepath.Join(dir, "safe_divide.sql"), []byte("IF($2 = 0, NULL, $1 / $2)"), 0644)
	os.WriteFile(filepath.Join(dir, "README.md"), []byte("not a function"), 0644)

	fm, err := LoadFunctions(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(fm) != 2 {
		t.Fatalf("expected 2 functions, got %d", len(fm))
	}

	if _, ok := fm["timestamp_to_utc"]; !ok {
		t.Error("timestamp_to_utc not found")
	}
	if _, ok := fm["safe_divide"]; !ok {
		t.Error("safe_divide not found")
	}

	// Test function invocation with argument substitution
	fn := fm["safe_divide"].(func(...string) string)
	result := fn("revenue", "count")
	expected := "IF(count = 0, NULL, revenue / count)"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestLoadFunctions_SkipsSubdirectories(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "subdir"), 0755)
	os.WriteFile(filepath.Join(dir, "subdir", "nested.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(dir, "top.sql"), []byte("SELECT 2"), 0644)

	fm, err := LoadFunctions(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(fm) != 1 {
		t.Errorf("expected 1 function, got %d", len(fm))
	}
}

func TestMergeFuncMaps(t *testing.T) {
	a := template.FuncMap{"foo": func() string { return "a" }}
	b := template.FuncMap{"bar": func() string { return "b" }}
	c := template.FuncMap{"foo": func() string { return "c" }}

	merged := MergeFuncMaps(a, b, c)
	if len(merged) != 2 {
		t.Errorf("expected 2 functions, got %d", len(merged))
	}
	// Later maps override earlier ones
	result := merged["foo"].(func() string)()
	if result != "c" {
		t.Errorf("expected 'c' (override), got %q", result)
	}
}

func TestFuncMapInTemplate(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "cast_to_cents.sql"), []byte("CAST($1 * 100 AS INT64)"), 0644)

	userFuncs, err := LoadFunctions(dir)
	if err != nil {
		t.Fatal(err)
	}
	fm := MergeFuncMaps(BuiltinFuncMap(), userFuncs)

	sql := `SELECT {{ cast_to_currency "price" }}, {{ cast_to_cents "price" }} FROM orders`
	tmpl, err := template.New("test").Funcs(fm).Parse(sql)
	if err != nil {
		t.Fatal(err)
	}

	var buf []byte
	w := &writerBuf{buf: &buf}
	if err := tmpl.Execute(w, nil); err != nil {
		t.Fatal(err)
	}

	result := string(*w.buf)
	if result != `SELECT CAST(ROUND(price, 2) AS NUMERIC), CAST(price * 100 AS INT64) FROM orders` {
		t.Errorf("unexpected result: %s", result)
	}
}

type writerBuf struct {
	buf *[]byte
}

func (w *writerBuf) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

func TestBuildRefFunc_BasicResolution(t *testing.T) {
	ctx := RefContext{
		Assets: []RefAsset{
			{Name: "orders", Layer: "analytics", Dataset: "legacy_analytics"},
			{Name: "stg_payments", Layer: "staging", Dataset: "legacy_staging"},
			{Name: "report_summary", Layer: "report", Dataset: "legacy_report"},
			{Name: "no_layer", Layer: "", Dataset: "default_ds"},
		},
		Datasets:       map[string]string{"analytics": "legacy_analytics", "staging": "legacy_staging", "report": "legacy_report"},
		DefaultDataset: "default_ds",
	}

	ref := BuildRefFunc(ctx)

	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{"analytics asset", "orders", "`legacy_analytics.orders`", false},
		{"staging asset", "stg_payments", "`legacy_staging.stg_payments`", false},
		{"report asset", "report_summary", "`legacy_report.report_summary`", false},
		{"no layer uses default", "no_layer", "`default_ds.no_layer`", false},
		{"nonexistent asset", "nonexistent", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ref(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for ref(%q)", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("ref(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestBuildRefFunc_WithPrefix(t *testing.T) {
	ctx := RefContext{
		Assets:         []RefAsset{{Name: "orders", Layer: "analytics", Dataset: "legacy_analytics"}},
		Datasets:       map[string]string{"analytics": "legacy_analytics"},
		DefaultDataset: "default_ds",
		Prefix:         "ah_",
	}

	ref := BuildRefFunc(ctx)
	got, err := ref("orders")
	if err != nil {
		t.Fatal(err)
	}
	if got != "`legacy_analytics.ah_orders`" {
		t.Errorf("ref with prefix = %q, want %q", got, "`legacy_analytics.ah_orders`")
	}
}

func TestBuildRefFunc_InTemplate(t *testing.T) {
	ctx := RefContext{
		Assets: []RefAsset{
			{Name: "orders", Layer: "analytics", Dataset: "legacy_analytics"},
			{Name: "payments", Layer: "staging", Dataset: "legacy_staging"},
		},
		Datasets:       map[string]string{"analytics": "legacy_analytics", "staging": "legacy_staging"},
		DefaultDataset: "default_ds",
	}

	fm := MergeFuncMaps(BuiltinFuncMap(), template.FuncMap{
		"ref": BuildRefFunc(ctx),
	})

	sql := `SELECT o.id, p.amount
FROM {{ ref "orders" }} o
JOIN {{ ref "payments" }} p ON o.id = p.order_id
WHERE o.dataset = '{{.Dataset}}'`

	tmpl, err := template.New("test").Funcs(fm).Parse(sql)
	if err != nil {
		t.Fatal(err)
	}

	data := templateData{Project: "my-project", Dataset: "legacy_analytics", Prefix: ""}
	var buf []byte
	w := &writerBuf{buf: &buf}
	if err := tmpl.Execute(w, data); err != nil {
		t.Fatal(err)
	}

	result := string(*w.buf)
	expected := "SELECT o.id, p.amount\nFROM `legacy_analytics.orders` o\nJOIN `legacy_staging.payments` p ON o.id = p.order_id\nWHERE o.dataset = 'legacy_analytics'"
	if result != expected {
		t.Errorf("template render:\ngot:  %s\nwant: %s", result, expected)
	}
}

func TestBuildRefFunc_NonexistentInTemplate(t *testing.T) {
	ctx := RefContext{
		Assets:         []RefAsset{{Name: "orders", Layer: "analytics", Dataset: "legacy_analytics"}},
		DefaultDataset: "default_ds",
	}
	fm := template.FuncMap{"ref": BuildRefFunc(ctx)}

	sql := `SELECT * FROM {{ ref "nonexistent" }}`
	tmpl, err := template.New("test").Funcs(fm).Parse(sql)
	if err != nil {
		t.Fatal(err)
	}

	var buf []byte
	w := &writerBuf{buf: &buf}
	err = tmpl.Execute(w, nil)
	if err == nil {
		t.Error("expected template execution error for ref to nonexistent asset")
	}
}
