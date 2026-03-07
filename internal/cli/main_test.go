package cli

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("MEMORI_PRINCIPAL", "llm")
	_ = os.Setenv("MEMORI_LLM_PROVIDER", "test")
	_ = os.Setenv("MEMORI_LLM_MODEL", "memori-cli")
	_ = os.Setenv("MEMORI_ALLOW_MANUAL_COMMAND_ID", "1")
	os.Exit(m.Run())
}
