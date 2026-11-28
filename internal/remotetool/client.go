package remotetool

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

var ErrResponseLost = errors.New("remote tool response unavailable")

type ClientConfig struct {
	Endpoint string
	Identity contract.RemoteToolIdentity
	TLS      *tls.Config
}

type Client struct {
	endpoint string
	identity contract.RemoteToolIdentity
	http     *http.Client
}

func NewClient(cfg ClientConfig) (*Client, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		!validIdentity(cfg.Identity) || cfg.TLS == nil || cfg.TLS.InsecureSkipVerify ||
		(len(cfg.TLS.Certificates) == 0 && cfg.TLS.GetClientCertificate == nil) {
		return nil, contract.ErrInvalidConfiguration
	}
	tlsConfig := cfg.TLS.Clone()
	tlsConfig.NextProtos = []string{"http/1.1"}
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig, DisableKeepAlives: true, DisableCompression: true,
		MaxResponseHeaderBytes: 16 << 10, TLSHandshakeTimeout: 5 * time.Second,
		// Each request gets its own HTTP/1 connection. There is no transport replay.
		Protocols: new(http.Protocols),
	}
	transport.Protocols.SetHTTP1(true)
	return &Client{endpoint: strings.TrimSuffix(endpoint.String(), "/"), identity: cfg.Identity,
		http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}}}, nil
}

func (c *Client) Identity() contract.RemoteToolIdentity { return c.identity }

// Mesh supplies the trusted run scope. Direct Tool calls cannot construct it.
func (*Client) Invoke(context.Context, contract.ToolRequest) (contract.ToolResult, error) {
	return contract.ToolResult{}, contract.ErrAccessDenied
}

func (*Client) QueryReceipt(context.Context, contract.ReceiptKey) (*contract.ToolReceipt, error) {
	return nil, contract.ErrAccessDenied
}

func (c *Client) InvokeRemote(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner, req contract.ToolRequest) (response, error) {
	return c.call(ctx, invokePath, request{Key: key, Scope: scope, Owner: owner, Tool: &req, Limits: req.Limits}, req.Limits.MaxResultBytes)
}

func (c *Client) QueryRemote(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner, limits contract.ToolLimits) (response, error) {
	return c.call(ctx, queryPath, request{Key: key, Scope: scope, Owner: owner, Limits: limits}, limits.MaxResultBytes)
}

func (c *Client) VerifyWorkspace(ctx context.Context, key contract.ReceiptKey, scope contract.RunScope, owner contract.RemoteOwner) error {
	reply, err := c.call(ctx, queryPath, request{Key: key, Scope: scope, Owner: owner, Workspace: true}, 0)
	if err != nil {
		return err
	}
	if !reply.Ready {
		return contract.ErrRecoveryUnavailable
	}
	return nil
}

func (c *Client) call(ctx context.Context, path string, message request, limit uint64) (response, error) {
	empty := response{}
	notSent := func(err error) (response, error) {
		return empty, errors.Join(contract.ErrNotDelivered, err)
	}
	if message.Key.Remote != c.identity {
		return notSent(contract.ErrRecoveryBinding)
	}
	if err := ctx.Err(); err != nil {
		return notSent(err)
	}
	message.ID = rand.Text()
	message.Deadline, _ = ctx.Deadline()
	body, err := json.Marshal(message)
	if err != nil || len(body) > maxRequestBytes {
		return notSent(contract.ErrInvalidRequest)
	}
	var connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return notSent(err)
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	wireReply, err := c.http.Do(req)
	if err != nil {
		if !connected.Load() {
			return notSent(err)
		}
		return empty, fmt.Errorf("%w: %v", ErrResponseLost, err)
	}
	defer wireReply.Body.Close()
	if wireReply.StatusCode != http.StatusOK {
		return empty, fmt.Errorf("%w: status %d", ErrResponseLost, wireReply.StatusCode)
	}
	var reply response
	if err := readMessage(wireReply.Body, int64(min(limit, maxReceiptBytes))+envelopeBytes, &reply); err != nil {
		return empty, fmt.Errorf("%w: %v", ErrResponseLost, err)
	}
	if reply.ID != message.ID || reply.Identity != c.identity ||
		(reply.Receipt != nil && !validReceipt(reply.Receipt, message.Key, limit)) ||
		(path == invokePath && reply.Receipt == nil) || (message.Workspace && reply.Receipt != nil) {
		return empty, ErrResponseLost
	}
	return reply, nil
}
