package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	dclient "github.com/sourcenetwork/defradb/client"
	dnode   "github.com/sourcenetwork/defradb/node"
	"github.com/rs/zerolog"
)

func defaultRootdir() string {
	if cwd, err := os.Getwd(); err == nil {
		return filepath.Join(cwd, ".defra-kv")
	}
	return ".defra-kv"
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		if h, err := os.UserHomeDir(); err == nil {
			return filepath.Join(h, p[2:])
		}
	}
	return p
}

func resolveRootdir(p string) string {
	p = expandHome(p)
	if !filepath.IsAbs(p) {
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
	}
	if err := os.MkdirAll(p, 0o755); err != nil {
		log.Fatalf("create dataConfigDir: %v", err)
	}
	return p
}

// Single JSON-based KV schema with indexes where useful.
const kvSchema = `
type KV {
  key: String @index
  value: JSON
  updatedAt: DateTime @index
}
`

func kvExists(ctx context.Context, n *dnode.Node) bool {
	res := n.DB.ExecRequest(ctx, `query { __type(name: "KV") { name } }`)
	if len(res.GQL.Errors) > 0 {
		return false
	}
	b, err := json.Marshal(res.GQL.Data)
	if err != nil {
		return false
	}
	return bytes.Contains(b, []byte(`"name":"KV"`))
}

func ensureKV(ctx context.Context, n *dnode.Node) error {
	if kvExists(ctx, n) {
		return nil
	}
	if _, err := n.DB.AddSchema(ctx, kvSchema); err != nil {
		return fmt.Errorf("KV schema add failed: %v", err)
	}
	return nil
}

type fdSilencer struct {
	muted         bool
	devnull       *os.File
	origStdout    *os.File
	origStderr    *os.File
	origLogWriter io.Writer
}

func (s *fdSilencer) Mute() {
	if s.muted {
		return
	}
	dn, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	s.devnull = dn
	s.origStdout = os.Stdout
	s.origStderr = os.Stderr
	s.origLogWriter = log.Writer()

	// redirect global stdio and stdlib logger
	os.Stdout = dn
	os.Stderr = dn
	log.SetOutput(dn)

	s.muted = true
}

func (s *fdSilencer) PrintlnOut(line string) {
	if s != nil && s.origStdout != nil {
		_, _ = s.origStdout.Write([]byte(line))
		_, _ = s.origStdout.Write([]byte("\n"))
		return
	}
	_, _ = os.Stdout.Write([]byte(line + "\n"))
}

func (s *fdSilencer) PrintlnErr(line string) {
	if s != nil && s.origStderr != nil {
		_, _ = s.origStderr.Write([]byte(line))
		_, _ = s.origStderr.Write([]byte("\n"))
		return
	}
	_, _ = os.Stderr.Write([]byte(line + "\n"))
}

func die(s *fdSilencer, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	if s != nil {
		s.PrintlnErr(msg)
	} else {
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(1)
}

func main() {
	// Flags
	fs := flag.NewFlagSet("defra-kv", flag.ExitOnError)
	dataConfigDir := fs.String("dir", defaultRootdir(), "Data/config directory")
	secret := fs.String("keyring-secret", "", "Keyring secret (sets DEFRA_KEYRING_SECRET)")
	query := fs.String("query", "", "GraphQL query/mutation")
	varsStr := fs.String("vars", "", "JSON variables")
	pretty := fs.Bool("pretty", true, "Pretty-print JSON output")
	reqTO := fs.Duration("timeout", 10*time.Second, "Request timeout")
	devMode := fs.Bool("dev", false, "Enable DefraDB development mode and verbose logging")
	_ = fs.Parse(os.Args[1:])

	// Keyring secret (first run convenience)
	if *secret != "" {
		_ = os.Setenv("DEFRA_KEYRING_SECRET", *secret)
	}
	if os.Getenv("DEFRA_KEYRING_SECRET") == "" {
		_ = os.Setenv("DEFRA_KEYRING_SECRET", "dev-dev-dev")
	}

	// Read query (flag or stdin)
	q := strings.TrimSpace(*query)
	if q == "" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatalf("read stdin: %v", err)
		}
		q = strings.TrimSpace(string(b))
	}
	if q == "" {
		fmt.Fprintln(os.Stderr, "no query provided; pass -query or pipe to stdin")
		os.Exit(2)
	}

	// Variables (optional)
	var vars map[string]any
	if v := strings.TrimSpace(*varsStr); v != "" {
		if err := json.Unmarshal([]byte(v), &vars); err != nil {
			log.Fatalf("parse -vars: %v", err)
		}
	}

	// Context + signals
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Configure logging based on dev mode
	var sil fdSilencer
	if !*devMode {
		// Environment-driven loggers used by Defra & deps
		_ = os.Setenv("DEFRA_LOG_LEVEL", "error")
		_ = os.Setenv("CORELOG_LEVEL", "error") // if corelog is present
		_ = os.Setenv("GOLOG_LOG_LEVEL", "error")

		// zerolog global level
		zerolog.SetGlobalLevel(zerolog.Disabled)

		// mute stdio
		sil.Mute()
	} else {
		// allow all logs through
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Create and start the node (embedded, persistent Badger)
	n, err := dnode.New(
		ctx,
		dnode.WithDisableAPI(true),                    // no HTTP server
		dnode.WithDisableP2P(true),                    // local only
		dnode.WithBadgerInMemory(false),               // persistent
		dnode.WithStoreType(dnode.BadgerStore),
		dnode.WithStorePath(resolveRootdir(*dataConfigDir)), // data dir
		dnode.WithLensRuntime(dnode.Wazero),           // pure-Go WASM runtime
		dnode.WithEnableDevelopment(*devMode),         // toggle dev features/logging
	)
	if err != nil {
		die(&sil, "dnode.New: %v", err)
	}
	defer func() {
		_ = n.Close(ctx)
	}()

	if err := n.Start(ctx); err != nil {
		die(&sil, "n.Start: %v", err)
	}

	if err := ensureKV(ctx, n); err != nil {
		die(&sil, "ensure KV schema: %v", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, *reqTO)
	defer cancel()

	res := n.DB.ExecRequest(reqCtx, q, dclient.WithVariables(vars))

	// Close the node explicitly
	_ = n.Close(ctx)

	// Output GraphQL errors as reported (if any)
	if len(res.GQL.Errors) > 0 {
		enc, _ := json.MarshalIndent(res.GQL.Errors, "", "  ")
		if !*devMode {
			sil.PrintlnErr(string(enc))
		} else {
			fmt.Fprintln(os.Stderr, string(enc))
		}
		os.Exit(1)
	}

	// Output JSON (with pretty-printing if specified)
	var outBytes []byte
	if *pretty {
		outBytes, _ = json.MarshalIndent(map[string]any{"data": res.GQL.Data}, "", "  ")
	} else {
		outBytes, _ = json.Marshal(map[string]any{"data": res.GQL.Data})
	}
	if !*devMode {
		sil.PrintlnOut(string(outBytes))
	} else {
		fmt.Println(string(outBytes))
	}
}
