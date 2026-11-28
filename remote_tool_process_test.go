package superagent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

const remoteInvokePath = "/mesh/v1/tool/invoke"
const remoteReceiptPath = "/mesh/v1/tool/receipt"

func makeRemoteCertificates(t *testing.T, root string) {
	t.Helper()
	now := time.Now()
	key := func() *ecdsa.PrivateKey {
		t.Helper()
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	newCertificate := func(id int64, name string) *x509.Certificate {
		return &x509.Certificate{SerialNumber: big.NewInt(id), Subject: pkix.Name{CommonName: name},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	}
	write := func(path, kind string, value []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, path), pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: value}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ca, caKey := newCertificate(1, "test Mesh CA"), key()
	ca.IsCA, ca.BasicConstraintsValid = true, true
	ca.KeyUsage |= x509.KeyUsageCertSign
	encodedCA, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	write("ca.pem", "CERTIFICATE", encodedCA)
	for i, role := range []string{"server", "owner", "reader", "other"} {
		cert, private := newCertificate(int64(i+2), role), key()
		if role == "server" {
			cert.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		encoded, err := x509.CreateCertificate(rand.Reader, cert, ca, &private.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		pk, err := x509.MarshalECPrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		write(role+".pem", "CERTIFICATE", encoded)
		write(role+"-key.pem", "EC PRIVATE KEY", pk)
	}
}

func loadRemoteTLS(t *testing.T, root, role string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(root, role+".pem"), filepath.Join(root, role+"-key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	pemCA, err := os.ReadFile(filepath.Join(root, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemCA) {
		t.Fatal("invalid test CA")
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if role == "server" {
		cfg.ClientCAs, cfg.ClientAuth = roots, tls.RequireAndVerifyClientCert
	} else {
		cfg.RootCAs = roots
	}
	return cfg
}

func remoteWorkspace(t *testing.T, root string) (EnvironmentProvider, RemoteToolIdentity) {
	t.Helper()
	env, err := NewLocalTakeoverEnvironment(map[WorkspaceRef]WorkspaceConfig{"project": {
		Root: filepath.Join(root, "workspace"), ReadPrefixes: []string{"."},
	}})
	if err != nil {
		t.Fatal(err)
	}
	version, err := env.(RecoveryWorkspace).RecoveryWorkspace(t.Context(), RunScope{Workspace: "project"})
	if err != nil {
		t.Fatal(err)
	}
	return env, RemoteToolIdentity{Provider: "remote-tools", Journal: "original-journal", Version: "v1",
		Workspace: "project", WorkspaceVersion: version}
}

func prepareRemoteAppend(args json.RawMessage) (string, error) {
	var input struct{ Path string }
	if err := json.Unmarshal(args, &input); err != nil || input.Path != "notes.txt" {
		return "", ErrInvalidRequest
	}
	return input.Path, nil
}

func TestRemoteToolServiceProcess(t *testing.T) {
	root := os.Getenv("SUPERAGENT_REMOTE_TOOL_TEST_DIRECTORY")
	if root == "" {
		return
	}
	role := os.Getenv("SUPERAGENT_REMOTE_TOOL_TEST_ROLE")
	scenario := os.Getenv("SUPERAGENT_REMOTE_TOOL_TEST_SCENARIO")
	env, identity := remoteWorkspace(t, root)
	log := &recoveryFixture{root: root}
	authority, err := NewSharedRemoteToolAuthority(filepath.Join(root, "runs"))
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := RemoteToolServerConfig{
		Identity: identity, ToolRef: "write", Environment: env, Authority: authority,
		ReceiptDirectory: filepath.Join(root, "receipts"),
		Authorize: func(_ context.Context, peer *x509.Certificate, key ReceiptKey) RemoteToolAccess {
			if key.Subject != "owner" || key.Runner != "runner" || key.BindingVersion != "v1" {
				return RemoteToolAccess{}
			}
			switch peer.Subject.CommonName {
			case "owner":
				return RemoteToolAccess{Invoke: true, ReadReceipt: true}
			case "reader":
				return RemoteToolAccess{ReadReceipt: true}
			default:
				return RemoteToolAccess{}
			}
		},
		Binding: LocalToolBinding{
			Spec:    ToolSpec{NeedsWorkspaceWrite: true, Limits: ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8}},
			Prepare: prepareRemoteAppend,
			Tool: deliveryTool(func(ctx context.Context, req ToolRequest) (ToolResult, error) {
				key, ok := InvocationReceiptKey(ctx)
				if !ok {
					return ToolResult{}, errors.New("missing durable invocation identity")
				}
				log.append("remote-executed", key)
				if err := PublishToolFile(ctx, req, "notes.txt", []byte("saved:"+string(req.CallID)+"\n")); err != nil {
					return ToolResult{}, err
				}
				if err := writeTakeoverMarker(root, "remote-file-written", "yes"); err != nil {
					return ToolResult{}, err
				}
				if scenario == "before-journal" || scenario == "disconnect-before-journal" {
					if err := waitTakeoverMarker(ctx, root, "release-journal"); err != nil {
						return ToolResult{}, err
					}
				}
				return ToolResult{RunID: req.RunID, CallID: req.CallID, State: ToolExecutionKnown,
					Completion: &ToolCompletion{Outcome: ToolSucceeded, Content: "saved"}}, nil
			}),
		},
	}
	if strings.HasPrefix(scenario, "evidence-") {
		environment, identity := remoteEvidenceWorkspace(t, root)
		serverConfig.Identity = identity
		serverConfig.Environment = &evidenceProcessWorkspace{
			EnvironmentProvider: environment, EvidenceWorkspace: environment.(contract.EvidenceWorkspace),
			root: root, scenario: scenario, log: log,
		}
		serverConfig.EvidenceRetention = time.Minute
		serverConfig.Binding = LocalToolBinding{Spec: ToolSpec{
			NeedsWorkspaceWrite: true, Limits: ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8},
		}}
	}
	handler, err := NewRemoteToolHandler(serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTakeoverMarker(root, "remote-url-"+role, "https://"+listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler, TLSConfig: loadRemoteTLS(t, root, "server"), ReadHeaderTimeout: 5 * time.Second}
	if err := server.ServeTLS(listener, "", ""); err != nil {
		t.Fatal(err)
	}
}

func launchRemoteService(t *testing.T, root, role, scenario string) (*exec.Cmd, string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRemoteToolServiceProcess$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_REMOTE_TOOL_TEST_DIRECTORY="+root,
		"SUPERAGENT_REMOTE_TOOL_TEST_ROLE="+role, "SUPERAGENT_REMOTE_TOOL_TEST_SCENARIO="+scenario)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exit error
	go func() { exit = cmd.Wait(); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	marker := "remote-url-" + role
	if err := waitTakeoverMarker(ctx, root, marker); err != nil {
		t.Fatalf("provider failed to start: %v", err)
	}
	addr, err := os.ReadFile(filepath.Join(root, marker))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatalf("provider exited: %v", exit)
	default:
	}
	return cmd, string(addr), stop
}

type remoteWireRequest struct {
	ID        string
	Key       ReceiptKey
	Scope     RunScope
	Owner     RemoteOwner
	Deadline  time.Time
	Tool      *ToolRequest
	Limits    ToolLimits
	Workspace bool
}

type remoteFaultProxy struct {
	root, scenario string
	target         atomic.Value
	sends, queries atomic.Int32
	dropped        atomic.Int32
	probeCount     atomic.Int32
	holdQueries    atomic.Bool
	server         *httptest.Server
	client         *http.Client
	log            recoveryFixture
}

func newRemoteProxy(t *testing.T, root, target, scenario string) *remoteFaultProxy {
	t.Helper()
	p := &remoteFaultProxy{root: root, scenario: scenario, log: recoveryFixture{root: root}}
	p.target.Store(target)
	transport := &http.Transport{TLSClientConfig: loadRemoteTLS(t, root, "owner"), DisableKeepAlives: true}
	p.client = &http.Client{Transport: transport}
	p.server = httptest.NewUnstartedServer(http.HandlerFunc(p.serve))
	p.server.TLS = loadRemoteTLS(t, root, "server")
	p.server.StartTLS()
	t.Cleanup(p.server.Close)
	return p
}

func (p *remoteFaultProxy) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	var msg remoteWireRequest
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "invalid test envelope", http.StatusBadRequest)
		return
	}
	invoke := r.URL.Path == remoteInvokePath
	var queryNumber int32
	switch {
	case invoke:
		p.sends.Add(1)
		p.log.append("remote-sends", msg)
	case msg.Workspace:
		p.probeCount.Add(1)
	default:
		queryNumber = p.queries.Add(1)
		p.log.append("remote-queries", msg)
		if queryNumber == 1 {
			p.snapshot("remote-before-query")
		}
	}
	if !invoke && !msg.Workspace && (p.holdQueries.Load() || p.scenario == "continuous-drop" || (p.scenario == "late-receipt" && queryNumber == 1)) {
		dropRemoteReply(w, nil)
		return
	}
	target, _ := p.target.Load().(string)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target+r.URL.Path, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, "request failed", http.StatusBadGateway)
		return
	}
	if invoke && p.scenario == "disconnect-before-journal" {
		go func() {
			reply, err := p.client.Do(req)
			if err == nil {
				_ = reply.Body.Close()
			}
		}()
		if waitTakeoverMarker(r.Context(), p.root, "remote-file-written") == nil {
			p.dropped.Add(1)
		}
		dropRemoteReply(w, nil)
		return
	}
	reply, err := p.client.Do(req)
	if err != nil {
		dropRemoteReply(w, nil)
		return
	}
	defer reply.Body.Close()
	data, err := io.ReadAll(reply.Body)
	if invoke && reply.StatusCode == http.StatusOK && err == nil && p.scenario != "disconnect-before-journal" && p.scenario != "stage-b-acceptance" {
		p.dropped.Add(1)
		receipts, _ := filepath.Glob(filepath.Join(p.root, "receipts", "*.receipt"))
		if len(receipts) == 1 {
			raw, readErr := os.ReadFile(receipts[0])
			var saved struct{ Result *ToolResult }
			if readErr == nil && json.Unmarshal(raw, &saved) == nil && saved.Result != nil {
				_ = writeTakeoverMarker(p.root, "remote-receipt-before-cut", string(saved.Result.CallID))
			}
		}
		if p.scenario == "workspace-unavailable" {
			_ = os.Rename(filepath.Join(p.root, "workspace"), filepath.Join(p.root, "original-workspace"))
			_ = os.Mkdir(filepath.Join(p.root, "workspace"), 0700)
		}
		dropRemoteReply(w, data)
		return
	}
	if !invoke && !msg.Workspace && reply.StatusCode == http.StatusOK && p.scenario == "wrong-provider" {
		var response map[string]json.RawMessage
		if json.Unmarshal(data, &response) == nil {
			response["Identity"] = json.RawMessage(`{"Provider":"different-provider"}`)
			data, _ = json.Marshal(response)
		}
	}
	if !invoke && !msg.Workspace && p.scenario == "oversized-query" {
		data = []byte(strings.Repeat(" ", 32<<10))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(reply.StatusCode)
	_, _ = w.Write(data)
}

func (p *remoteFaultProxy) snapshot(name string) {
	paths, _ := filepath.Glob(filepath.Join(p.root, "runs", "*.run"))
	if len(paths) != 1 {
		return
	}
	data, err := os.ReadFile(paths[0])
	if err == nil {
		_ = writeTakeoverMarker(p.root, name, string(data))
	}
}

func readRemoteSnapshot(t *testing.T, root, name string) fixtureCheckpoint {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint fixtureCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func dropRemoteReply(w http.ResponseWriter, body []byte) {
	conn, wire, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	if len(body) > 0 {
		_, _ = fmt.Fprintf(wire, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: application/json\r\n\r\n", len(body))
		_, _ = wire.Write(body[:len(body)/2])
		_ = wire.Flush()
	}
}

func remoteRunClient(t *testing.T, root, endpoint string, identity RemoteToolIdentity, model ModelProvider, window time.Duration, takeover bool) *Client {
	t.Helper()
	m, err := NewMesh(remoteRunConfig(t, root, endpoint, identity, model, window, takeover))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(m, "runner")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func remoteRunConfig(t *testing.T, root, endpoint string, identity RemoteToolIdentity, model ModelProvider, window time.Duration, takeover bool) MeshConfig {
	t.Helper()
	tool, err := NewRemoteTool(RemoteToolClientConfig{Endpoint: endpoint, Identity: identity, TLS: loadRemoteTLS(t, root, "owner")})
	if err != nil {
		t.Fatal(err)
	}
	budget := Budget{Timeout: address(10 * time.Second), MaxModelRequests: address(uint64(2)), MaxModelInputBytes: address(uint64(8192)),
		MaxOutputBytes: address(uint64(16384)), MaxToolRequests: address(uint64(1)), MaxToolScanBytes: address(uint64(16))}
	recovery := &RecoveryConfig{Directory: filepath.Join(root, "runs"), BindingVersion: "v1", ReconcileTimeout: window}
	if takeover {
		recovery.Takeover = &TakeoverConfig{LeaseDuration: time.Second}
	}
	prepare := prepareRemoteAppend
	if identity.EvidenceRule != "" {
		prepare = PrepareEvidenceAppend
	}
	return MeshConfig{
		LocalRunners: map[RunnerRef]RunnerConfig{"runner": {
			AllowedModels: map[SubjectID][]ModelRef{"owner": {"model"}},
			AllowedTools:  map[SubjectID]map[ToolRef]ToolPermission{"owner": {"write": {}}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
			ContextPolicies: map[ModelRef]ModelContextPolicy{"model": {MaxOutputTokens: 20}},
			Recovery:        recovery,
		}},
		LocalModels: map[ModelRef]ModelProvider{"model": model},
		LocalTools: map[ToolRef]LocalToolBinding{"write": {
			Tool: tool, Prepare: prepare,
			Spec: ToolSpec{InputSchema: json.RawMessage(`{"type":"object"}`), Limits: ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8}},
		}},
		ResolveCaller: func(context.Context) (SubjectID, error) { return "owner", nil },
	}
}

func TestRemoteToolRecoveryAcrossRealConnection(t *testing.T) {
	requireLocalRecovery(t)
	for _, tc := range []struct {
		scenario string
		outcome  RunOutcome
		cleanup  CleanupStatus
		takeover bool
	}{
		{"late-receipt", Succeeded, CleanupReleased, false},
		{"takeover-receipt", Succeeded, CleanupReleased, true},
		{"disconnect-before-journal", Succeeded, CleanupReleased, false},
		{"before-journal", ToolOutcomeUnknown, CleanupPending, false},
		{"continuous-drop", ToolOutcomeUnknown, CleanupPending, false},
		{"wrong-provider", ToolOutcomeUnknown, CleanupPending, false},
		{"oversized-query", ToolOutcomeUnknown, CleanupPending, false},
		{"workspace-unavailable", Failed, CleanupReleased, false},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			root := makeRecoveryRoot(t)
			makeRemoteCertificates(t, root)
			_, identity := remoteWorkspace(t, root)
			_, endpoint, stop := launchRemoteService(t, root, "original", tc.scenario)
			proxy := newRemoteProxy(t, root, endpoint, tc.scenario)
			model := defaultRecoveryFixture(root)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			client := remoteRunClient(t, root, proxy.server.URL, identity, model, 1050*time.Millisecond, tc.takeover)
			handle, err := client.Start(ctx, StartRequest{ID: recoveryID, Model: "model", Recoverable: true, Takeover: tc.takeover,
				Tools: []ToolRef{"write"}, Input: UserMessage{Text: "save once"}})
			if err != nil {
				t.Fatal(err)
			}
			if tc.scenario == "before-journal" {
				if err := waitTakeoverMarker(ctx, root, "remote-file-written"); err != nil {
					t.Fatal(err)
				}
				beforePaths, _ := filepath.Glob(filepath.Join(root, "receipts", "*.receipt"))
				if len(beforePaths) != 1 {
					t.Fatalf("expected one pending original receipt: %v", beforePaths)
				}
				data, err := os.ReadFile(beforePaths[0])
				if err != nil || !strings.Contains(string(data), `"Result":null`) {
					t.Fatalf("tool reached the wrong crash window: %s (%v)", data, err)
				}
				stop()
				_, replacement, _ := launchRemoteService(t, root, "replacement", "normal")
				proxy.target.Store(replacement)
			}
			if tc.scenario == "disconnect-before-journal" {
				if err := waitTakeoverMarker(ctx, root, "remote-before-query"); err != nil {
					t.Fatal(err)
				}
				if err := writeTakeoverMarker(root, "release-journal", "yes"); err != nil {
					t.Fatal(err)
				}
			}
			result, err := handle.Wait(ctx)
			if err != nil || result.Outcome != tc.outcome || result.Cleanup != tc.cleanup {
				t.Fatalf("Wait = %+v (%v)", result, err)
			}
			executed := readRecoveryLog[ReceiptKey](t, root, "remote-executed")
			sent := readRecoveryLog[remoteWireRequest](t, root, "remote-sends")
			queried := readRecoveryLog[remoteWireRequest](t, root, "remote-queries")
			models := readRecoveryLog[ModelRequest](t, root, "models")
			if proxy.sends.Load() != 1 || len(sent) != 1 || len(executed) != 1 || len(queried) == 0 ||
				executed[0] != sent[0].Key || executed[0].Remote != identity {
				t.Fatalf("wrong invocation identity or counts: send=%v execute=%v query=%v", sent, executed, queried)
			}
			ids := map[string]bool{sent[0].ID: true}
			for _, q := range queried {
				if q.Key != sent[0].Key || q.ID == "" || ids[q.ID] || q.Tool != nil || q.Workspace {
					t.Fatalf("query did not target the original call read-only: %+v", q)
				}
				ids[q.ID] = true
			}
			name := "workspace"
			if tc.scenario == "workspace-unavailable" {
				name = "original-workspace"
			}
			data, err := os.ReadFile(filepath.Join(root, name, "notes.txt"))
			if err != nil || string(data) != "saved:c1\n" {
				t.Fatalf("physical writes = %q (%v)", data, err)
			}
			final := readRecoveryCheckpoint(t, root)
			before := readRemoteSnapshot(t, root, "remote-before-query")
			if final.Balance.ToolRequests != 0 || before.ReconcileUntil.IsZero() ||
				!before.Deadline.Equal(final.Deadline) || !before.ReconcileUntil.Before(before.Deadline) {
				t.Fatalf("missing original reservation or absolute reconciliation deadline: before %+v, final %+v", before, final)
			}
			switch tc.outcome {
			case Succeeded:
				if tc.scenario != "disconnect-before-journal" {
					proof, err := os.ReadFile(filepath.Join(root, "remote-receipt-before-cut"))
					if err != nil || string(proof) != "c1" {
						t.Fatalf("cut before the receipt was durable: %q (%v)", proof, err)
					}
				}
				if len(models) != 2 || models[1].Step != 2 || len(models[1].Messages) != 3 ||
					models[1].Messages[2].Tool.Result.CallID != "c1" || result.Answer.Text != "done" || len(result.Messages) != 4 {
					t.Fatalf("checkpoint did not advance exactly once: %v, %+v", models, result)
				}
				wire, _ := json.Marshal(models[1].Messages[2].Tool.Result)
				if models[1].MaxOutputBytes != before.Balance.Output-uint64(len(wire)) {
					t.Fatalf("budget did not settle one receipt: %d -> %d, receipt %d",
						before.Balance.Output, models[1].MaxOutputBytes, len(wire))
				}
				if proxy.dropped.Load() != 1 || (tc.scenario == "late-receipt" && len(queried) < 2) {
					t.Fatalf("fault injection was not exercised: dropped %d queries %d", proxy.dropped.Load(), len(queried))
				}
			case ToolOutcomeUnknown:
				if len(models) != 1 || len(result.Messages) != 1 || result.Stop.CallID != "c1" || result.Stop.Trigger != StopReconcileDeadline {
					t.Fatalf("uncertain remote call advanced: models=%v result=%+v", models, result)
				}
			case Failed:
				if len(models) != 1 || result.Stop.Code != StopWorkspaceUnavailable || proxy.probeCount.Load() == 0 {
					t.Fatalf("unavailable workspace allowed dependent work: %+v", result)
				}
			}
			fresh := remoteRunClient(t, root, proxy.server.URL, identity, model, time.Second, tc.takeover)
			original, err := fresh.Recover(ctx, handle.ID())
			if err != nil {
				t.Fatal(err)
			}
			repeated, err := original.Wait(ctx)
			if err != nil || !reflect.DeepEqual(result, repeated) || proxy.sends.Load() != 1 || proxy.queries.Load() != int32(len(queried)) {
				t.Fatalf("terminal changed on repeat: %+v (%v)", repeated, err)
			}
		})
	}
}

type remoteFixtureReply struct {
	ID       string
	Identity RemoteToolIdentity
	Receipt  *ToolReceipt
}

func sendRemoteWire(t *testing.T, root, endpoint, role, path string, msg remoteWireRequest) (int, remoteFixtureReply) {
	t.Helper()
	msg.Deadline = time.Now().Add(time.Second)
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: loadRemoteTLS(t, root, role), DisableKeepAlives: true}
	reply, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	var wire remoteFixtureReply
	if reply.StatusCode == http.StatusOK {
		if err := json.NewDecoder(reply.Body).Decode(&wire); err != nil {
			t.Fatal(err)
		}
	}
	return reply.StatusCode, wire
}

func TestRemoteToolProviderAuthorizationAndDeduplication(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	_, identity := remoteWorkspace(t, root)
	_, endpoint, _ := launchRemoteService(t, root, "auth", "normal")
	req := ToolRequest{RunID: "owned-run", CallID: "c1", Tool: "write", Arguments: json.RawMessage(`{"path":"notes.txt"}`),
		Limits: ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8}}
	key := ReceiptKey{Remote: identity, Runner: "runner", Subject: "owner", RunID: req.RunID, Step: 1,
		CallID: req.CallID, Tool: req.Tool, BindingVersion: "v1", Fingerprint: durable.ToolFingerprint(req, "notes.txt")}
	scope := RunScope{Runner: key.Runner, Subject: key.Subject, RunID: key.RunID, Deadline: time.Now().Add(8 * time.Second)}
	invoke := remoteWireRequest{ID: "original", Key: key, Scope: scope, Tool: &req, Limits: req.Limits}
	query := remoteWireRequest{ID: "query", Key: key, Scope: scope, Limits: req.Limits}
	if status, _ := sendRemoteWire(t, root, endpoint, "reader", remoteInvokePath, invoke); status != http.StatusForbidden {
		t.Fatalf("read-only principal invoked: %d", status)
	}
	if status, _ := sendRemoteWire(t, root, endpoint, "other", remoteReceiptPath, query); status != http.StatusForbidden {
		t.Fatalf("unrelated principal read a receipt: %d", status)
	}
	if status, reply := sendRemoteWire(t, root, endpoint, "reader", remoteReceiptPath, query); status != http.StatusOK || reply.Receipt != nil {
		t.Fatalf("unregistered invocation receipt: %d %+v", status, reply)
	}
	status, initial := sendRemoteWire(t, root, endpoint, "owner", remoteInvokePath, invoke)
	if status != http.StatusOK || initial.Receipt == nil || initial.Receipt.Key != key {
		t.Fatalf("original execution: %d %+v", status, initial)
	}
	invoke.ID = "deliberate-duplicate"
	status, duplicate := sendRemoteWire(t, root, endpoint, "owner", remoteInvokePath, invoke)
	if status != http.StatusOK || !reflect.DeepEqual(initial.Receipt, duplicate.Receipt) || duplicate.ID != invoke.ID {
		t.Fatalf("duplicate did not return the original receipt: %d %+v", status, duplicate)
	}
	status, lookedUp := sendRemoteWire(t, root, endpoint, "reader", remoteReceiptPath, query)
	if status != http.StatusOK || !reflect.DeepEqual(initial.Receipt, lookedUp.Receipt) {
		t.Fatalf("read-only query: %d %+v", status, lookedUp)
	}
	req.Arguments = json.RawMessage(`{"path":"notes.txt","other":1}`)
	invoke.Key.Fingerprint = durable.ToolFingerprint(req, "notes.txt")
	invoke.ID = "conflicting-duplicate"
	if status, _ := sendRemoteWire(t, root, endpoint, "owner", remoteInvokePath, invoke); status != http.StatusConflict {
		t.Fatalf("conflicting fingerprint was not rejected: %d", status)
	}
	data, err := os.ReadFile(filepath.Join(root, "workspace", "notes.txt"))
	if err != nil || string(data) != "saved:c1\n" || len(readRecoveryLog[ReceiptKey](t, root, "remote-executed")) != 1 {
		t.Fatalf("explicit duplicates produced extra work: %q %v", data, err)
	}
}

func TestRemoteToolUnsentConnectionFailure(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	_, identity := remoteWorkspace(t, root)
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "https://" + dead.Addr().String()
	if err := dead.Close(); err != nil {
		t.Fatal(err)
	}
	model := defaultRecoveryFixture(root)
	client := remoteRunClient(t, root, endpoint, identity, model, time.Second, false)
	handle, err := client.Start(t.Context(), StartRequest{ID: recoveryID, Model: "model", Recoverable: true, Tools: []ToolRef{"write"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	result, err := handle.Wait(ctx)
	if err != nil || result.Outcome != Succeeded || result.Cleanup != CleanupReleased || len(result.Messages) != 4 {
		t.Fatalf("pre-connection failure = %+v (%v)", result, err)
	}
	tool := result.Messages[2].Tool.Result
	if tool.State != ToolExecutionNotExecuted || tool.Reason.Code != "request_not_delivered" ||
		!readRecoveryCheckpoint(t, root).ReconcileUntil.IsZero() {
		t.Fatalf("unsent request entered reconciliation: %+v", tool)
	}
	if _, err := os.Stat(filepath.Join(root, "workspace", "notes.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsent request produced output: %v", err)
	}
}

type remotePauseModel struct{ *recoveryFixture }

func (f remotePauseModel) MeasureInput(ctx context.Context, req ModelRequest) (InputTokenMeasurement, error) {
	if req.Step == 2 {
		if err := writeTakeoverMarker(f.root, "remote-agent-batch-committed", "yes"); err != nil {
			return InputTokenMeasurement{}, err
		}
		<-ctx.Done()
		return InputTokenMeasurement{}, ctx.Err()
	}
	return f.recoveryFixture.MeasureInput(ctx, req)
}

func TestRemoteToolRunnerProcess(t *testing.T) {
	root := os.Getenv("SUPERAGENT_REMOTE_RUNNER_TEST_DIRECTORY")
	if root == "" {
		return
	}
	role := os.Getenv("SUPERAGENT_REMOTE_RUNNER_TEST_ROLE")
	scenario := os.Getenv("SUPERAGENT_REMOTE_RUNNER_TEST_SCENARIO")
	takeover := scenario == "ownership" || scenario == "evidence-fenced-stage"
	f := defaultRecoveryFixture(root)
	var model ModelProvider = f
	if role == "original" && scenario == "committed" {
		model = remotePauseModel{f}
	}
	_, identity := remoteWorkspace(t, root)
	if scenario == "evidence-fenced-stage" {
		_, identity = remoteEvidenceWorkspace(t, root)
		model = writeEvidenceModel{f}
	}
	client := remoteRunClient(t, root, os.Getenv("SUPERAGENT_REMOTE_RUNNER_TEST_ENDPOINT"), identity, model, 3800*time.Millisecond, takeover)
	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Second)
	defer cancel()
	var handle *Run
	var err error
	if role == "original" {
		handle, err = client.Start(ctx, StartRequest{ID: recoveryID, Model: "model", Recoverable: true, Takeover: takeover,
			Tools: []ToolRef{"write"}, Input: UserMessage{Text: "save once"}})
	} else {
		for {
			handle, err = client.Recover(ctx, recoveryID)
			if !errors.Is(err, ErrRecoveryBusy) {
				break
			}
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			case <-timer.C:
			}
		}
	}
	if err != nil || handle.ID() != recoveryID {
		t.Fatalf("run admission: %v (%v)", handle, err)
	}
	if err := writeTakeoverMarker(root, "remote-runner-accepted-"+role, "yes"); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.append("remote-runner-results", result)
}

func launchRemoteRunner(t *testing.T, root, endpoint, role, scenario string) (stop func(), wait func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRemoteToolRunnerProcess$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_REMOTE_RUNNER_TEST_DIRECTORY="+root, "SUPERAGENT_REMOTE_RUNNER_TEST_ENDPOINT="+endpoint,
		"SUPERAGENT_REMOTE_RUNNER_TEST_ROLE="+role, "SUPERAGENT_REMOTE_RUNNER_TEST_SCENARIO="+scenario)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var status error
	go func() { status = cmd.Wait(); close(done) }()
	stop = func() { cancel(); <-done }
	wait = func() {
		<-done
		if status != nil {
			t.Fatalf("%s runner: %v", role, status)
		}
	}
	t.Cleanup(stop)
	return stop, wait
}

func TestRemoteToolRecoveryUsesPersistedPositionAfterRunnerRestart(t *testing.T) {
	requireLocalRecovery(t)
	for _, scenario := range []string{"pending", "committed", "ownership"} {
		t.Run(scenario, func(t *testing.T) {
			root := makeRecoveryRoot(t)
			makeRemoteCertificates(t, root)
			_, endpoint, _ := launchRemoteService(t, root, "provider", "normal")
			proxy := newRemoteProxy(t, root, endpoint, "cut")
			proxy.holdQueries.Store(scenario != "committed")
			stop, _ := launchRemoteRunner(t, root, proxy.server.URL, "original", scenario)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			marker := "remote-before-query"
			if scenario == "committed" {
				marker = "remote-agent-batch-committed"
			}
			if err := waitTakeoverMarker(ctx, root, marker); err != nil {
				t.Fatal(err)
			}
			before := readRecoveryCheckpoint(t, root)
			if scenario == "committed" && (before.Step != 2 || before.Intent != nil) ||
				scenario != "committed" && (before.Intent == nil || before.ReconcileUntil.IsZero()) {
				t.Fatalf("unexpected recovery position: %+v", before)
			}
			stop()
			oldQueries := proxy.queries.Load()
			_, wait := launchRemoteRunner(t, root, proxy.server.URL, "recover", scenario)
			if err := waitTakeoverMarker(ctx, root, "remote-runner-accepted-recover"); err != nil {
				t.Fatal(err)
			}
			if scenario != "committed" {
				afterAdmission := readRecoveryCheckpoint(t, root)
				if !before.ReconcileUntil.Equal(afterAdmission.ReconcileUntil) || !before.Deadline.Equal(afterAdmission.Deadline) {
					t.Fatalf("restart renewed the absolute deadline: before %+v after %+v", before, afterAdmission)
				}
				if scenario == "ownership" && (before.Owner.Epoch != 1 || afterAdmission.Owner.Epoch != 2) {
					t.Fatalf("owner was not replaced: before %+v after %+v", before.Owner, afterAdmission.Owner)
				}
			}
			proxy.holdQueries.Store(false)
			wait()
			models := readRecoveryLog[ModelRequest](t, root, "models")
			results := readRecoveryLog[Result](t, root, "remote-runner-results")
			executed := readRecoveryLog[ReceiptKey](t, root, "remote-executed")
			if len(results) != 1 || results[0].Outcome != Succeeded || len(models) != 2 || len(executed) != 1 || proxy.sends.Load() != 1 {
				t.Fatalf("recovery did not continue exactly once: models %v results %v invokes %d", models, results, proxy.sends.Load())
			}
			if scenario == "committed" {
				if proxy.queries.Load() != oldQueries || models[1].MaxOutputBytes != before.Balance.Output {
					t.Fatal("already settled remote call was queried or charged again")
				}
			} else {
				tool, _ := json.Marshal(models[1].Messages[2].Tool.Result)
				if models[1].MaxOutputBytes != before.Balance.Output-uint64(len(tool)) {
					t.Fatal("pending call was not settled once after restart")
				}
			}
			if scenario == "ownership" {
				var accepted bool
				for _, q := range readRecoveryLog[remoteWireRequest](t, root, "remote-queries") {
					accepted = accepted || q.Owner.Epoch == 2 && q.Key.AdmissionEpoch == 1
				}
				if !accepted {
					t.Fatal("current owner did not query the original admission generation")
				}
			}
		})
	}
}

func TestRemoteToolCancellationKeepsUnknownOutcome(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	_, identity := remoteWorkspace(t, root)
	_, endpoint, _ := launchRemoteService(t, root, "provider", "normal")
	proxy := newRemoteProxy(t, root, endpoint, "continuous-drop")
	model := defaultRecoveryFixture(root)
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	client := remoteRunClient(t, root, proxy.server.URL, identity, model, 3*time.Second, false)
	handle, err := client.Start(ctx, StartRequest{ID: recoveryID, Model: "model", Recoverable: true, Tools: []ToolRef{"write"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := waitTakeoverMarker(ctx, root, "remote-before-query"); err != nil {
		t.Fatal(err)
	}
	wait, stopWait := context.WithTimeout(ctx, time.Millisecond)
	defer stopWait()
	if _, err := handle.Wait(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("brief Wait: %v", err)
	}
	if err := handle.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(ctx)
	if err != nil || result.Outcome != ToolOutcomeUnknown || result.Stop.Trigger != StopCancelled || result.Stop.CallID != "c1" ||
		proxy.sends.Load() != 1 || len(readRecoveryLog[ModelRequest](t, root, "models")) != 1 {
		t.Fatalf("canceled reconciliation: %+v (%v)", result, err)
	}
}
