// Package sink pushes line-protocol chunks into Arc via the bulk import
// endpoint POST /api/v1/import/lp.
//
// Verified contract (Arc internal/api/import.go):
//   - multipart/form-data, file field name "file"
//   - gzip auto-detected by magic bytes (0x1f 0x8b); we always gzip
//   - target database via header "x-arc-database" (or ?db=)
//   - precision via ?precision=ns|us|ms|s (we emit ns)
//   - admin-tier token: Authorization: Bearer <token>
//   - 500 MB limit on BOTH compressed and decompressed bytes (so the
//     DECOMPRESSED chunk must be <500 MB; gzip saves bandwidth only)
//   - on success: HTTP 200 {"status":"ok","result":{"rows_imported":N,...}};
//     the handler FlushAll()s before returning, so 2xx == durably persisted.
package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"time"
)

// Sink posts LP chunks to one Arc cluster.
type Sink struct {
	baseURL   string
	token     string
	precision string
	client    *http.Client

	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// Option configures a Sink.
type Option func(*Sink)

// WithRetry sets retry policy (attempts and backoff bounds).
func WithRetry(maxRetries int, base, max time.Duration) Option {
	return func(s *Sink) {
		s.maxRetries = maxRetries
		s.baseDelay = base
		s.maxDelay = max
	}
}

// WithHTTPClient overrides the default tuned client (used by tests).
func WithHTTPClient(c *http.Client) Option {
	return func(s *Sink) { s.client = c }
}

// New builds a Sink. baseURL is Arc's root (e.g. https://arc.example.net),
// token is an admin-tier API token, precision is the LP timestamp precision.
func New(baseURL, token, precision string, opts ...Option) *Sink {
	s := &Sink{
		baseURL:    trimSlash(baseURL),
		token:      token,
		precision:  precision,
		maxRetries: 6,
		baseDelay:  time.Second,
		maxDelay:   60 * time.Second,
		client: &http.Client{
			Timeout: 10 * time.Minute, // a 450 MB chunk can take a while
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:          32,
				MaxIdleConnsPerHost:   8,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				ForceAttemptHTTP2:     true,
			},
		},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Result is the parsed Arc import response.
type Result struct {
	Status string `json:"status"`
	Result struct {
		Database     string   `json:"database"`
		Measurements []string `json:"measurements"`
		RowsImported int64    `json:"rows_imported"`
		Precision    string   `json:"precision"`
		DurationMs   int64    `json:"duration_ms"`
	} `json:"result"`
}

// Send gzips lp and POSTs it to Arc targeting database db. It retries transient
// failures (429, 5xx, network errors) with exponential backoff. A 4xx other
// than 429 is permanent and returned immediately. The lp slice is the RAW
// (uncompressed) line protocol for one chunk; the caller must keep it <500 MB.
func (s *Sink) Send(ctx context.Context, db string, lp []byte) (Result, error) {
	body, contentType, err := buildMultipartGzip(lp)
	if err != nil {
		return Result{}, fmt.Errorf("build request body: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/import/lp?precision=%s", s.baseURL, s.precision)

	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, s.backoff(attempt)); err != nil {
				return Result{}, err
			}
		}
		res, retryable, err := s.doOnce(ctx, url, db, contentType, body)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !retryable {
			return Result{}, err
		}
	}
	return Result{}, fmt.Errorf("giving up after %d attempts: %w", s.maxRetries+1, lastErr)
}

// doOnce performs a single attempt. retryable indicates whether a failure is
// worth retrying.
func (s *Sink) doOnce(ctx context.Context, url, db, contentType string, body []byte) (res Result, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Result{}, false, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-arc-database", db)
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// network/transport error — transient
		return Result{}, true, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusOK:
		var r Result
		if err := json.Unmarshal(respBody, &r); err != nil {
			// 200 but unparseable body — treat as success with empty result
			return Result{Status: "ok"}, false, nil
		}
		return r, false, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return Result{}, true, fmt.Errorf("arc 429 rate limited: %s", snippet(respBody))
	case resp.StatusCode >= 500:
		return Result{}, true, fmt.Errorf("arc %d: %s", resp.StatusCode, snippet(respBody))
	default:
		// 4xx (auth, bad request, 413 too large) — permanent
		return Result{}, false, fmt.Errorf("arc %d (permanent): %s", resp.StatusCode, snippet(respBody))
	}
}

// backoff returns an exponential delay with full jitter. Jitter matters under
// concurrent workers: without it, many shards that hit a 429 at the same moment
// would retry in lockstep and re-create the thundering herd. Full jitter picks a
// uniform delay in [0, exp] so retries spread out. math/rand is fine here — this
// is load-shaping, not security.
func (s *Sink) backoff(attempt int) time.Duration {
	d := s.baseDelay << (attempt - 1) // 1,2,4,8,...
	if d > s.maxDelay || d <= 0 {
		d = s.maxDelay
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(d)))
}

// buildMultipartGzip wraps gzipped lp in a multipart body with field "file".
func buildMultipartGzip(lp []byte) (body []byte, contentType string, err error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "chunk.lp.gz")
	if err != nil {
		return nil, "", err
	}
	gz := gzip.NewWriter(fw)
	if _, err := gz.Write(lp); err != nil {
		return nil, "", err
	}
	if err := gz.Close(); err != nil {
		return nil, "", err
	}
	if err := mw.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), mw.FormDataContentType(), nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func snippet(b []byte) string {
	const max = 300
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
