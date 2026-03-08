package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestInstallReleaseScriptInstallsFromLocalReleaseDir(t *testing.T) {
	t.Parallel()

	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", ".."))
	installScript := filepath.Join(repoRoot, "scripts", "install_release.sh")

	goos := runtime.GOOS
	goarch := runtime.GOARCH
	version := "v9.9.9"

	releaseDir := filepath.Join(t.TempDir(), "release")
	stageDir := filepath.Join(releaseDir, "memori_"+version+"_"+goos+"_"+goarch)
	binDir := filepath.Join(stageDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create bin dir: %v", err)
	}

	installedPayload := "#!/usr/bin/env bash\necho memori installer smoke\n"
	if err := os.WriteFile(filepath.Join(binDir, "memori"), []byte(installedPayload), 0o755); err != nil {
		t.Fatalf("write fake memori binary: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stageDir, "README.md"), []byte("release readme\n"), 0o644); err != nil {
		t.Fatalf("write release readme: %v", err)
	}

	archivePath := filepath.Join(releaseDir, "memori_"+version+"_"+goos+"_"+goarch+".tar.gz")
	tarCmd := exec.Command("tar", "-C", releaseDir, "-czf", archivePath, filepath.Base(stageDir))
	if output, err := tarCmd.CombinedOutput(); err != nil {
		t.Fatalf("create release archive: %v\n%s", err, string(output))
	}
	if err := os.RemoveAll(stageDir); err != nil {
		t.Fatalf("remove staging dir: %v", err)
	}

	installDir := filepath.Join(t.TempDir(), "bin")
	cmd := exec.Command("bash", installScript, "--version", version, "--dir", installDir, "--base-url", releaseDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run install script: %v\n%s", err, string(output))
	}

	installedBinary := filepath.Join(installDir, "memori")
	info, err := os.Stat(installedBinary)
	if err != nil {
		t.Fatalf("stat installed binary: %v", err)
	}
	if info.Mode()&0o111 == 0 {
		t.Fatalf("expected installed binary to be executable, mode=%v", info.Mode())
	}

	runCmd := exec.Command(installedBinary)
	output, err := runCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run installed binary: %v\n%s", err, string(output))
	}
	if string(output) != "memori installer smoke\n" {
		t.Fatalf("unexpected installed binary output: %q", string(output))
	}
}
