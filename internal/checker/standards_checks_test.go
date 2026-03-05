package checker

import (
	"strings"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
)

func TestGenerateStandardsCheckNodes_AllTypes(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_users",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "user_id",
				DestinationConnection: "bq",
				Standards: &config.StandardsConfig{
					Email:    []string{"email"},
					Phone:    []string{"phone_number"},
					Currency: []string{"balance"},
				},
			},
		},
	}

	nodes, deps := GenerateStandardsCheckNodes(cfg)

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes (email + phone + currency), got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}

	expected := []string{
		"check:stg_users:default:standards_email_email",
		"check:stg_users:default:standards_phone_phone_number",
		"check:stg_users:default:standards_currency_balance",
	}
	for _, e := range expected {
		if !names[e] {
			t.Errorf("missing check: %s", e)
		}
	}

	for _, n := range nodes {
		if n.Type != "sql_check" {
			t.Errorf("expected type sql_check, got %q for %s", n.Type, n.Name)
		}
		if n.InlineSQL == "" {
			t.Errorf("expected InlineSQL for %s", n.Name)
		}
		if n.DestinationConnection != "bq" {
			t.Errorf("expected DestinationConnection bq, got %q for %s", n.DestinationConnection, n.Name)
		}
		if n.SourceAsset != "stg_users" {
			t.Errorf("expected SourceAsset stg_users, got %q for %s", n.SourceAsset, n.Name)
		}
		if n.Blocking {
			t.Errorf("expected non-blocking by default, got blocking for %s", n.Name)
		}
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "stg_users" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "email") {
			if !strings.Contains(n.InlineSQL, "LOWER(TRIM(email))") {
				t.Errorf("email SQL missing LOWER(TRIM): %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "EMAIL_NOT_NORMALIZED") {
				t.Errorf("email SQL missing issue_type: %s", n.InlineSQL)
			}
		}
		if strings.Contains(n.Name, "phone") {
			if !strings.Contains(n.InlineSQL, "REGEXP_CONTAINS") {
				t.Errorf("phone SQL missing REGEXP_CONTAINS: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "PHONE_FORMAT_INVALID") {
				t.Errorf("phone SQL missing issue_type: %s", n.InlineSQL)
			}
		}
		if strings.Contains(n.Name, "currency") {
			if !strings.Contains(n.InlineSQL, "ROUND(balance, 2)") {
				t.Errorf("currency SQL missing ROUND: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "CURRENCY_NOT_ROUNDED") {
				t.Errorf("currency SQL missing issue_type: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateStandardsCheckNodes_Blocking(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_users",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "user_id",
				DestinationConnection: "bq",
				StandardsBlocking:     true,
				Standards: &config.StandardsConfig{
					Email: []string{"email"},
				},
			},
		},
	}

	nodes, _ := GenerateStandardsCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if !nodes[0].Blocking {
		t.Errorf("expected blocking when standards_blocking is true")
	}
}

func TestGenerateStandardsCheckNodes_NoStandards(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "order_id",
				DestinationConnection: "bq",
			},
		},
	}

	nodes, deps := GenerateStandardsCheckNodes(cfg)

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for asset with no standards, got %d", len(nodes))
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps for asset with no standards, got %d", len(deps))
	}
}

func TestGenerateStandardsCheckNodes_MultipleColumns(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_users",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "user_id",
				DestinationConnection: "bq",
				Standards: &config.StandardsConfig{
					Email: []string{"email", "secondary_email"},
				},
			},
		},
	}

	nodes, _ := GenerateStandardsCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes for 2 email columns, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:stg_users:default:standards_email_email"] {
		t.Error("missing check for email column")
	}
	if !names["check:stg_users:default:standards_email_secondary_email"] {
		t.Error("missing check for secondary_email column")
	}
}
