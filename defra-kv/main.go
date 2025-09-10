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
	"regexp"
	"strings"
	"syscall"
	"time"

	dclient "github.com/sourcenetwork/defradb/client"
	dnode "github.com/sourcenetwork/defradb/node"
)

// Single JSON-based KV schema with indexes where useful.
const kvSchema = `
type KV {
	key: String @index(unique: true)
	value: JSON
	updatedAt: DateTime @index
}
`

var gqlNameRE = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)

func defaultDataConfigDir() string {
	if cwd, err := os.Getwd(); err == nil {
		return filepath.Join(cwd, ".defra-kv")
	}
	return ".defra-kv"
}

func resolveRootdir(p string) string {
	if p == "" || p == "." {
		p = defaultDataConfigDir()
	}
	if err := os.MkdirAll(p, 0o755); err != nil {
		log.Fatalf("create dataConfigDir: %v", err)
	}
	return p
}

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

type errSilencer struct {
	muted         bool
	devnull       *os.File
	origStderr    *os.File
}

func (s *errSilencer) Mute() {
	if s.muted {
		return
	}
	dn, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	s.devnull = dn
	s.origStderr = os.Stderr

	// redirect global stderr
	os.Stderr = dn

	s.muted = true
}

func (s *errSilencer) PrintlnErr(line string) {
	if s != nil && s.origStderr != nil {
		_, _ = s.origStderr.Write([]byte(line))
		_, _ = s.origStderr.Write([]byte("\n"))
		return
	}
	fmt.Fprintln(os.Stderr, line)
}

func die(s *errSilencer, format string, a ...any) {
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
	hasKey := fs.String("has", "", "Check key existence")
	getKey := fs.String("get", "", "Get value by key")
	setKey := fs.String("set", "", "Set/update value by key (value via stdin)")
	removeKey := fs.String("remove", "", "Remove key/value")
	varsStr := fs.String("vars", "", "JSON variables")
	query := fs.String("query", "", "Raw GraphQL query/mutation")
	pretty := fs.Bool("pretty", true, "Pretty-print JSON output")
	reqTO := fs.Duration("timeout", 10*time.Second, "Request timeout")
	dataConfigDir := fs.String("dir", defaultDataConfigDir(), "Data/config directory")
	secret := fs.String("keyring-secret", "", "Keyring secret (sets DEFRA_KEYRING_SECRET)")
	devMode := fs.Bool("dev", false, "Enable DefraDB development mode and verbose logging")
	_ = fs.Parse(os.Args[1:])

	// Determine mode: raw (-query) takes precedence over KV actions
	hasAction := (
		strings.TrimSpace(*query) == "" &&
		(*setKey != "" || *getKey != "" || *hasKey != "" || *removeKey != ""))

	// Keyring secret (first run convenience)
	if *secret != "" {
		_ = os.Setenv("DEFRA_KEYRING_SECRET", *secret)
	}
	if os.Getenv("DEFRA_KEYRING_SECRET") == "" {
		_ = os.Setenv("DEFRA_KEYRING_SECRET", "dev-dev-dev")
	}

	// Read query (flag or stdin) for raw mode only
	var q string
	if !hasAction {
		q = strings.TrimSpace(*query)
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
	}

	// Variables (optional) for raw mode
	var vars map[string]any
	if !hasAction {
		if v := strings.TrimSpace(*varsStr); v != "" {
			if err := json.Unmarshal([]byte(v), &vars); err != nil {
				log.Fatalf("parse -vars: %v", err)
			}
		}
	}

	// Context + signals
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Configure logging based on dev mode
	var sil errSilencer
	if !*devMode {
		_ = os.Setenv("LOG_LEVEL", "error")
		sil.Mute()
	} else {
		_ = os.Setenv("LOG_LEVEL", "info")
	}

	// Create and start the node (embedded, persistent Badger)
	n, err := dnode.New(
		ctx,
		dnode.WithDisableAPI(true),                          // no HTTP server
		dnode.WithDisableP2P(true),                          // local only
		dnode.WithBadgerInMemory(false),                     // persistent
		dnode.WithStoreType(dnode.BadgerStore),
		dnode.WithStorePath(resolveRootdir(*dataConfigDir)), // data dir
		dnode.WithLensRuntime(dnode.Wazero),                 // pure-Go WASM runtime
		dnode.WithEnableDevelopment(*devMode),               // toggle dev features/logging
	)
	if err != nil {
		die(&sil, "dnode.New: %v", err)
	}

	if err := n.Start(ctx); err != nil {
		die(&sil, "n.Start: %v", err)
	}

	if err := ensureKV(ctx, n); err != nil {
		die(&sil, "ensure KV schema: %v", err)
	}

	// Build canned KV queries if in action mode
	if hasAction {
		var b []byte
		var err error
		vars = map[string]any{}

		if *setKey != "" {
			// Read value JSON from stdin
			b, err = io.ReadAll(os.Stdin)
			if err != nil {
				die(&sil, "read stdin value: %v", err)
			}
			valStr := strings.TrimSpace(string(b))
			if valStr == "" {
				die(&sil, "no value on stdin for -set")
			}
			var val any
			if err := json.Unmarshal([]byte(valStr), &val); err != nil {
				die(&sil, "invalid JSON on stdin: %v", err)
			}
			now := time.Now().UTC().Format(time.RFC3339Nano)
			vars["now"] = now
			vars["key"] = *setKey
			vars["value"] = val

			q = `mutation setKV($key:String!,$value:JSON!,$now:DateTime!) {
				upsert_KV(
					filter: { key: { _eq: $key } }
					create: { key: $key, value: $value, updatedAt: $now }
					update: { value: $value, updatedAt: $now }
				) { _docID }
			}`
		} else if *getKey != "" {
			vars["key"] = *getKey
			q = "query getKV($key:String!) { KV(filter:{ key:{ _eq:$key } }) { key value } }"
		} else if *hasKey != "" {
			vars["key"] = *hasKey
			q = "query hasKV($key:String!) { KV(filter:{ key:{ _eq:$key } }) { _docID } }"
		} else if *removeKey != "" {
			vars["key"] = *removeKey
			q = "mutation removeKV($key:String!) { delete_KV(filter:{ key:{ _eq:$key } }) { _docID } }"
		}
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

	// If using action mode, handle outputs/exit codes and return early
	if hasAction {
		m, _ := res.GQL.Data.(map[string]any)

		// set
		if *setKey != "" {
			rows, _ := m["upsert_KV"].([]map[string]any)
			if len(rows) > 0 {
				os.Exit(0)
			}
			os.Exit(3)
		}
		// has
		if *hasKey != "" {
			rows, _ := m["KV"].([]map[string]any)
			if len(rows) > 0 {
				os.Exit(0)
			}
			os.Exit(3)
		}
		// del
		if *removeKey != "" {
			rows, _ := m["delete_KV"].([]map[string]any)
			if len(rows) > 0 {
				os.Exit(0)
			}
			os.Exit(3)
		}
		// get
		if *getKey != "" {
			rows, _ := m["KV"].([]map[string]any)

			if len(rows) == 0 {
				os.Exit(3)
			}
			doc := rows[0]

			var outBytes []byte
			if *pretty {
				outBytes, _ = json.MarshalIndent(doc, "", "  ")
			} else {
				outBytes, _ = json.Marshal(doc)
			}

			fmt.Println(string(outBytes))
			os.Exit(0)
		}
	} else {
		// Output JSON (with pretty-printing if specified) for raw mode
		var outBytes []byte
		if *pretty {
			outBytes, _ = json.MarshalIndent(map[string]any{"data": res.GQL.Data}, "", "  ")
		} else {
			outBytes, _ = json.Marshal(map[string]any{"data": res.GQL.Data})
		}

		fmt.Println(string(outBytes))
		os.Exit(0)
	}
}
