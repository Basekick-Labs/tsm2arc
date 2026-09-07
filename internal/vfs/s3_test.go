package vfs

import (
	"context"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// fakeStore spins an in-process S3 with a bucket holding an InfluxDB-3-shaped
// key set, and returns an *S3 rooted at s3://store/<prefix>.
func fakeStore(t *testing.T, prefix string, keys map[string]string) *S3 {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket("store"); err != nil {
		t.Fatal(err)
	}
	for k, v := range keys {
		if _, err := backend.PutObject("store", k, nil, strings.NewReader(v), int64(len(v)), nil); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	url := "s3://store"
	if prefix != "" {
		url += "/" + prefix
	}
	f, err := NewS3(context.Background(), url, S3Options{Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

var storeKeys = map[string]string{
	"stores/prod/node0/wal/00000000001.wal":                         "idb3.001xxxx",
	"stores/prod/node0/wal/00000000002.wal":                         "idb3.001yyyy",
	"stores/prod/node0/snapshots/18446744073709551614.info.json":    `{"version":"1"}`,
	"stores/prod/node0/dbs/1/0/2026-09-07/18-52/0000000015.parquet": "hello world!",
	"stores/prod/other-junk.txt":                                    "not a node",
}

func TestS3ListRecursiveSorted(t *testing.T) {
	f := fakeStore(t, "stores/prod", storeKeys)
	got, err := f.List("node0/wal")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"node0/wal/00000000001.wal", "node0/wal/00000000002.wal"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	if got, _ := f.List("node0/nope"); got != nil {
		t.Fatalf("missing prefix List = %v, want empty", got)
	}
	all, err := f.List("node0")
	if err != nil || len(all) != 4 {
		t.Fatalf("recursive List = %v (err %v), want 4 objects", all, err)
	}
}

func TestS3ListDirAndHasPrefix(t *testing.T) {
	f := fakeStore(t, "stores/prod", storeKeys)
	dirs, err := f.ListDir("")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dirs, []string{"node0"}) {
		t.Fatalf("ListDir root = %v, want [node0] (files are not dirs)", dirs)
	}
	dirs, err = f.ListDir("node0")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dirs, []string{"dbs", "snapshots", "wal"}) {
		t.Fatalf("ListDir node0 = %v", dirs)
	}
	if !HasPrefix(f, "node0/wal") || HasPrefix(f, "node0/catalog") {
		t.Fatal("HasPrefix wrong on S3")
	}
}

func TestS3ReadFileAndRangedReaderAt(t *testing.T) {
	f := fakeStore(t, "stores/prod", storeKeys)
	b, err := f.ReadFile("node0/snapshots/18446744073709551614.info.json")
	if err != nil || string(b) != `{"version":"1"}` {
		t.Fatalf("ReadFile = (%q, %v)", b, err)
	}

	ra, size, err := f.ReaderAt("node0/dbs/1/0/2026-09-07/18-52/0000000015.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer ra.Close()
	if size != 12 {
		t.Fatalf("size = %d, want 12", size)
	}
	buf := make([]byte, 5)
	if _, err := ra.ReadAt(buf, 6); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf) != "world" {
		t.Fatalf("range read = %q", buf)
	}
	// Reads crossing EOF return the short data with io.EOF, like os.File.
	n, err := ra.ReadAt(buf, 10)
	if n != 2 || err != io.EOF || string(buf[:n]) != "d!" {
		t.Fatalf("tail read = (%d, %v, %q)", n, err, buf[:n])
	}
	if _, err := ra.ReadAt(buf, 99); err != io.EOF {
		t.Fatalf("past-EOF read err = %v, want io.EOF", err)
	}
}

func TestS3Rebase(t *testing.T) {
	f := fakeStore(t, "stores/prod/node0", storeKeys) // URL points AT the node
	if got := f.Prefix(); got != "stores/prod/node0" {
		t.Fatalf("prefix = %q", got)
	}
	re := NewS3Rebased(f, "stores/prod")
	if !HasPrefix(re, "node0/wal") {
		t.Fatal("rebased FS cannot see node-prefixed paths")
	}
	if re.URL() != "s3://store/stores/prod" {
		t.Fatalf("rebased URL = %q", re.URL())
	}
}
