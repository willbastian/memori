package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMain(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		cmd := helperMainCommand(t)
		cmd.Args = append(cmd.Args, "--")
		cmd.Env = append(cmd.Env, "MEMORI_MAIN_HELPER_ARGS=")

		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run main success helper: %v\noutput:\n%s", err, out)
		}
		if !strings.Contains(string(out), "Human Workflows:") {
			t.Fatalf("expected help output from empty invocation, got:\n%s", out)
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		cmd := helperMainCommand(t)
		cmd.Env = append(cmd.Env, "MEMORI_MAIN_HELPER_ARGS=not-a-real-command")

		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("expected helper command to fail, output:\n%s", out)
		}
		if !strings.Contains(string(out), "error: unknown command") {
			t.Fatalf("expected unknown command error, got:\n%s", out)
		}
	})
}

func TestMainHelperProcess(t *testing.T) {
	if os.Getenv("MEMORI_MAIN_HELPER_PROCESS") != "1" {
		return
	}

	args := strings.Fields(os.Getenv("MEMORI_MAIN_HELPER_ARGS"))
	os.Args = append([]string{"memori-test"}, args...)
	main()
}

func helperMainCommand(t *testing.T) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestMainHelperProcess")
	cmd.Env = append(os.Environ(),
		"MEMORI_MAIN_HELPER_PROCESS=1",
		"MEMORI_PRINCIPAL=llm",
		"MEMORI_LLM_PROVIDER=test",
		"MEMORI_LLM_MODEL=cmd-main",
		"MEMORI_ALLOW_MANUAL_COMMAND_ID=1",
	)
	return cmd
}
