package cli

import "testing"

func TestResolveContinuityModeCoversEnvDefaultAndInvalidInput(t *testing.T) {
	t.Setenv("MEMORI_CONTINUITY_MODE", "assist")

	if got, err := resolveContinuityMode(""); err != nil || got != continuityModeAssist {
		t.Fatalf("expected env-backed assist mode, got %q err=%v", got, err)
	}
	if got, err := resolveContinuityMode(" manual "); err != nil || got != continuityModeManual {
		t.Fatalf("expected explicit manual mode, got %q err=%v", got, err)
	}
	if _, err := resolveContinuityMode("nope"); err == nil {
		t.Fatal("expected invalid continuity mode error")
	}
}

func TestShellQuoteCoversPlainAndQuotedValues(t *testing.T) {
	if got := shellQuote(""); got != "''" {
		t.Fatalf("expected empty shell quote, got %q", got)
	}
	if got := shellQuote("plain-value"); got != "plain-value" {
		t.Fatalf("expected plain value to stay unquoted, got %q", got)
	}
	if got := shellQuote("needs space"); got != "'needs space'" {
		t.Fatalf("expected spaced value to be quoted, got %q", got)
	}
	if got := shellQuote("it's"); got != `'it'"'"'s'` {
		t.Fatalf("expected embedded quote to be escaped, got %q", got)
	}
}
