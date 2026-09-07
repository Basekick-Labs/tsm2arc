package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DirLister is the optional shallow-listing side of an FS: the immediate
// children of a prefix, without walking the whole subtree. Layout detection
// uses it — a recursive List of a store root would enumerate every data
// object just to find the node prefix.
type DirLister interface {
	// ListDir returns the names (not full paths) of the immediate child
	// "directories" of prefix, sorted. A missing prefix returns an empty
	// list.
	ListDir(prefix string) ([]string, error)
}

// prefixChecker is the optional cheap existence probe; see HasPrefix.
type prefixChecker interface {
	hasPrefix(prefix string) bool
}

// S3 is the s3:// implementation of FS: a read-only view of
// s3://bucket/prefix. All FS paths are relative to that prefix.
//
// Credentials and region come from the standard AWS chain (environment,
// shared config/credentials files, IMDS/IRSA). A custom endpoint (MinIO,
// on-prem gateways) forces path-style addressing.
type S3 struct {
	client *s3.Client
	bucket string
	prefix string // "" or "a/b" — no leading/trailing slash
	ctx    context.Context
}

// S3Options tunes NewS3 beyond the standard AWS configuration chain.
type S3Options struct {
	// Endpoint overrides the S3 endpoint URL (e.g. http://localhost:9000
	// for MinIO) and switches to path-style addressing.
	Endpoint string
	// Region overrides the region from the credential chain.
	Region string
}

// NewS3 opens a read-only view of an s3://bucket[/prefix] URL.
func NewS3(ctx context.Context, rawURL string, opt S3Options) (*S3, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("invalid S3 URL %q: want s3://bucket[/prefix]", rawURL)
	}
	var loadOpts []func(*awsconfig.LoadOptions) error
	if opt.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opt.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opt.Endpoint != "" {
			o.BaseEndpoint = aws.String(opt.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &S3{
		client: client,
		bucket: u.Host,
		prefix: strings.Trim(u.Path, "/"),
		ctx:    ctx,
	}, nil
}

// URL reconstructs the store URL (for messages).
func (s *S3) URL() string {
	if s.prefix == "" {
		return "s3://" + s.bucket
	}
	return "s3://" + s.bucket + "/" + s.prefix
}

// NewS3Rebased returns a view of the same bucket rooted at an ABSOLUTE key
// prefix — used to normalize a URL that points at the node prefix back up to
// the store root.
func NewS3Rebased(s *S3, prefix string) *S3 {
	cp := *s
	cp.prefix = strings.Trim(prefix, "/")
	return &cp
}

// Prefix returns the current key prefix inside the bucket.
func (s *S3) Prefix() string { return s.prefix }

func (s *S3) key(path string) string { return joinKey(s.prefix, path) }

func joinKey(prefix, path string) string {
	path = strings.Trim(path, "/")
	if prefix == "" {
		return path
	}
	if path == "" {
		return prefix
	}
	return prefix + "/" + path
}

// List implements FS: every object key under prefix, recursive, sorted.
func (s *S3) List(prefix string) ([]string, error) {
	full := s.key(prefix)
	if full != "" {
		full += "/"
	}
	var out []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(full),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(s.ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", s.URL()+"/"+prefix, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			rel := strings.TrimPrefix(key, s.prefix)
			out = append(out, strings.TrimPrefix(rel, "/"))
		}
	}
	sort.Strings(out) // ListObjectsV2 is key-ordered already; keep the contract explicit
	return out, nil
}

// ListDir implements DirLister via a delimiter listing (one request per
// 1000 children instead of walking the subtree).
func (s *S3) ListDir(prefix string) ([]string, error) {
	full := s.key(prefix)
	if full != "" {
		full += "/"
	}
	var out []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(full),
		Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(s.ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", s.URL()+"/"+prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), full), "/")
			if name != "" {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// hasPrefix probes for any object under prefix with a single MaxKeys=1
// request — the cheap existence check HasPrefix() routes here.
func (s *S3) hasPrefix(prefix string) bool {
	full := s.key(prefix)
	if full != "" {
		full += "/"
	}
	out, err := s.client.ListObjectsV2(s.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(full),
		MaxKeys: aws.Int32(1),
	})
	return err == nil && len(out.Contents) > 0
}

// ReadFile implements FS.
func (s *S3) ReadFile(path string) ([]byte, error) {
	obj, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(path)),
	})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", s.URL(), path, err)
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}

// ReaderAt implements FS: size from a HEAD request, then one ranged GET per
// ReadAt call — exactly the access pattern parquet footer/row-group reads
// want against an object store. Safe for concurrent ReadAt.
func (s *S3) ReaderAt(path string) (ReaderAtCloser, int64, error) {
	head, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(path)),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("head %s/%s: %w", s.URL(), path, err)
	}
	size := aws.ToInt64(head.ContentLength)
	return &s3ReaderAt{s: s, key: s.key(path), size: size}, size, nil
}

type s3ReaderAt struct {
	s    *S3
	key  string
	size int64
}

func (r *s3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	obj, err := r.s.client.GetObject(r.s.ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.s.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
	})
	if err != nil {
		return 0, fmt.Errorf("range get %s: %w", r.key, err)
	}
	defer obj.Body.Close()
	n, err := io.ReadFull(obj.Body, p[:end-off+1])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return n, err
	}
	if int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

func (r *s3ReaderAt) Close() error { return nil }
