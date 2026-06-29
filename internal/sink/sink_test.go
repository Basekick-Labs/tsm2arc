package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeArc mimics the parts of Arc's /api/v1/import/lp contract the sink relies
// on: multipart "file" field, gzip magic-byte detection, x-arc-database header,
// Bearer auth, and a {"status":"ok","result":{...}} response.
func fakeArc(t *testing.T, onLP func(db string, lp []byte)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/import/lp" {
			http.Error(w, "not found", 404)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			return
		}
		db := r.Header.Get("x-arc-database")
		if db == "" {
			http.Error(w, `{"error":"no db"}`, 400)
			return
		}
		f, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, `{"error":"no file"}`, 400)
			return
		}
		defer f.Close()
		data, _ := io.ReadAll(f)
		// gzip magic bytes
		if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
			http.Error(w, `{"error":"not gzip"}`, 400)
			return
		}
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			http.Error(w, `{"error":"bad gzip"}`, 400)
			return
		}
		lp, _ := io.ReadAll(gz)
		rows := bytes.Count(lp, []byte("\n"))
		if onLP != nil {
			onLP(db, lp)
		}
		fmt.Fprintf(w, `{"status":"ok","result":{"database":%q,"rows_imported":%d,"precision":"ns"}}`, db, rows)
	}))
}

func TestSinkSendSuccess(t *testing.T) {
	var gotDB string
	var gotLP []byte
	srv := fakeArc(t, func(db string, lp []byte) { gotDB = db; gotLP = lp })
	defer srv.Close()

	s := New(srv.URL, "test-token", "ns")
	lp := []byte("cpu,host=a usage=1.0 1\ncpu,host=a usage=2.0 2\n")
	res, err := s.Send(context.Background(), "telemetry", lp)
	if err != nil {
		t.Fatal(err)
	}
	if gotDB != "telemetry" {
		t.Errorf("db routed wrong: %q", gotDB)
	}
	if !bytes.Equal(gotLP, lp) {
		t.Errorf("lp roundtrip mismatch:\n got %q\nwant %q", gotLP, lp)
	}
	if res.Result.RowsImported != 2 {
		t.Errorf("rows imported %d want 2", res.Result.RowsImported)
	}
}

func TestSinkRetriesOn5xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			http.Error(w, `{"error":"transient"}`, 503)
			return
		}
		_ = r.Body.Close
		fmt.Fprint(w, `{"status":"ok","result":{"rows_imported":1}}`)
	}))
	defer srv.Close()

	s := New(srv.URL, "test-token", "ns", WithRetry(5, time.Millisecond, 10*time.Millisecond))
	_, err := s.Send(context.Background(), "db", []byte("m v=1 1\n"))
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Errorf("expected 3 calls (2 fail + 1 ok), got %d", calls)
	}
}

func TestSinkPermanentOn4xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, `{"error":"bad request"}`, 400)
	}))
	defer srv.Close()

	s := New(srv.URL, "test-token", "ns", WithRetry(5, time.Millisecond, 10*time.Millisecond))
	_, err := s.Send(context.Background(), "db", []byte("m v=1 1\n"))
	if err == nil {
		t.Fatal("expected permanent error on 400")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("4xx should not retry; got %d calls", calls)
	}
}

func TestSinkAuthRequired(t *testing.T) {
	srv := fakeArc(t, nil)
	defer srv.Close()
	s := New(srv.URL, "wrong-token", "ns", WithRetry(1, time.Millisecond, time.Millisecond))
	_, err := s.Send(context.Background(), "db", []byte("m v=1 1\n"))
	if err == nil {
		t.Fatal("expected auth failure")
	}
}
