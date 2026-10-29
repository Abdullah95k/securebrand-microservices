package browser

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"
)

// A minimal Chrome DevTools Protocol client: enough to drive one headless
// Chromium per render (navigate, read the DOM, cookies and network
// traffic, answer proxy authentication). One process per render keeps
// identities apart: a fresh profile, its own proxy.

const devtoolsOrigin = "http://127.0.0.1"

// message is a CDP command, response or event.
type message struct {
	ID        int64           `json:"id,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// event is a CDP event of the page session.
type event struct {
	Method string
	Params json.RawMessage
}

// conn is a DevTools websocket connection.
type conn struct {
	ws      *websocket.Conn
	next    atomic.Int64
	mu      sync.Mutex
	pending map[int64]chan message
	events  chan event
	done    chan struct{}
	err     error
}

func dial(ctx context.Context, wsURL string) (*conn, error) {
	cfg, err := websocket.NewConfig(wsURL, devtoolsOrigin)
	if err != nil {
		return nil, err
	}
	ws, err := cfg.DialContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("browser: devtools: %w", err)
	}
	ws.MaxPayloadBytes = 64 << 20
	c := &conn{ws: ws, pending: map[int64]chan message{}, events: make(chan event, 4096), done: make(chan struct{})}
	go c.read()
	return c, nil
}

func (c *conn) read() {
	defer close(c.done)
	for {
		var m message
		if err := websocket.JSON.Receive(c.ws, &m); err != nil {
			c.mu.Lock()
			c.err = err
			for id, ch := range c.pending {
				close(ch)
				delete(c.pending, id)
			}
			c.mu.Unlock()
			return
		}
		if m.ID != 0 {
			c.mu.Lock()
			ch := c.pending[m.ID]
			delete(c.pending, m.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- m
			}
			continue
		}
		select {
		case c.events <- event{Method: m.Method, Params: m.Params}:
		default:
			// A render that stopped reading events: drop rather than block
			// command responses.
		}
	}
}

// call sends a command and waits for its result.
func (c *conn) call(ctx context.Context, session, method string, params any, out any) error {
	id := c.next.Add(1)
	m := message{ID: id, SessionID: session, Method: method}
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		m.Params = b
	}
	ch := make(chan message, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return fmt.Errorf("browser: devtools closed: %w", c.err)
	}
	c.pending[id] = ch
	c.mu.Unlock()
	if err := websocket.JSON.Send(c.ws, m); err != nil {
		return fmt.Errorf("browser: %s: %w", method, err)
	}
	select {
	case r, ok := <-ch:
		if !ok {
			return fmt.Errorf("browser: %s: devtools closed", method)
		}
		if r.Error != nil {
			return fmt.Errorf("browser: %s: %s", method, r.Error.Message)
		}
		if out != nil && len(r.Result) > 0 {
			return json.Unmarshal(r.Result, out)
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *conn) close() { _ = c.ws.Close() }

// process is a running headless Chromium.
type process struct {
	cmd     *exec.Cmd
	profile string
	wsURL   string
}

// launch starts Chromium with a fresh profile and the DevTools endpoint on
// a free port.
func launch(ctx context.Context, binary string, noSandbox bool, args []string) (*process, error) {
	profile, err := os.MkdirTemp("", "fbs-browser-")
	if err != nil {
		return nil, err
	}
	flags := []string{
		"--headless=new", "--remote-debugging-port=0", "--user-data-dir=" + profile,
		// The DevTools client sends this Origin; Chromium refuses others.
		"--remote-allow-origins=" + devtoolsOrigin,
		"--no-first-run", "--no-default-browser-check", "--disable-gpu", "--disable-extensions",
		"--disable-background-networking", "--disable-sync", "--disable-dev-shm-usage",
		"--mute-audio", "--hide-scrollbars", "--window-size=1366,900",
	}
	if noSandbox || os.Geteuid() == 0 || os.Getenv("FBS_BROWSER_NO_SANDBOX") != "" {
		flags = append(flags, "--no-sandbox") // Chromium refuses to run as root with its sandbox
	}
	flags = append(flags, args...)
	flags = append(flags, "about:blank")
	cmd := exec.CommandContext(ctx, binary, flags...) //nolint:gosec // the binary is configuration
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(profile)
		return nil, fmt.Errorf("browser: start %s: %w", binary, err)
	}
	p := &process{cmd: cmd, profile: profile}
	// Chromium writes the port and the browser target path once listening.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if f, err := os.Open(filepath.Join(profile, "DevToolsActivePort")); err == nil { //nolint:gosec // our own temp profile
			sc := bufio.NewScanner(f)
			var lines []string
			for sc.Scan() {
				lines = append(lines, strings.TrimSpace(sc.Text()))
			}
			_ = f.Close()
			if len(lines) >= 2 && lines[0] != "" && lines[1] != "" {
				p.wsURL = "ws://127.0.0.1:" + lines[0] + lines[1]
				return p, nil
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			p.stop()
			return nil, errors.New("browser: chromium did not open its DevTools port")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *process) stop() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		_, _ = p.cmd.Process.Wait()
	}
	_ = os.RemoveAll(p.profile)
}
