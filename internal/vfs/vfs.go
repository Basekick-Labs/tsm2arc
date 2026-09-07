// Package vfs is the read-only source abstraction for data stores.
//
// InfluxDB 1.x/2.x data dirs are local by construction, but InfluxDB 3 writes
// its entire persistence tree — WAL, parquet, manifests, catalog — to an
// object store, and with anything other than `--object-store file` that tree
// lives in S3/GCS/Azure, not on a mountable volume. Reading it in place
// (object listings + range reads, which parquet wants anyway) is the product
// story; syncing terabytes to a scratch volume first is the workaround. This
// package is the seam: file:// today, s3:// next, with the same three
// operations either way.
//
// All paths are store-relative, '/'-separated, no leading slash — the object
// key convention, matched on local disk by joining against the root.
package vfs

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS is a minimal read-only object store view.
type FS interface {
	// List returns every object path under prefix (recursive), sorted
	// lexicographically. A missing prefix is not an error — it returns an
	// empty list, matching object-store semantics where "directories" don't
	// exist apart from their objects.
	List(prefix string) ([]string, error)
	// ReadFile returns an entire small object (manifests, catalog files, WAL).
	ReadFile(path string) ([]byte, error)
	// ReaderAt opens an object for random access (parquet footers + row
	// groups). The caller must Close it.
	ReaderAt(path string) (ReaderAtCloser, int64, error)
}

// ReaderAtCloser is what parquet readers need from an object.
type ReaderAtCloser interface {
	io.ReaderAt
	io.Closer
}

// Local is the file:// implementation rooted at a directory.
type Local struct {
	root string
}

// NewLocal returns an FS over the given root directory.
func NewLocal(root string) *Local { return &Local{root: root} }

func (l *Local) abs(path string) string {
	return filepath.Join(l.root, filepath.FromSlash(path))
}

// List implements FS. Unreadable subtrees abort with the underlying error —
// silently skipping objects in a migration source is data loss.
func (l *Local) List(prefix string) ([]string, error) {
	base := l.abs(prefix)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		return nil, nil
	}
	var out []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// ReadFile implements FS.
func (l *Local) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(l.abs(path))
}

// ReaderAt implements FS.
func (l *Local) ReaderAt(path string) (ReaderAtCloser, int64, error) {
	f, err := os.Open(l.abs(path))
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// HasPrefix reports whether any object exists under prefix — the cheap
// existence probe layout detection uses.
func HasPrefix(f FS, prefix string) bool {
	// For Local this stats the directory instead of walking it.
	if l, ok := f.(*Local); ok {
		fi, err := os.Stat(l.abs(prefix))
		return err == nil && fi.IsDir()
	}
	objs, err := f.List(strings.TrimSuffix(prefix, "/"))
	return err == nil && len(objs) > 0
}
