# tsm2arc is built by GoReleaser (static, CGO_ENABLED=0); this image just
# packages the prebuilt binary. Distroless static gives a tiny, CVE-light image
# with a CA bundle (needed for HTTPS to Arc) and no shell/package manager.
FROM gcr.io/distroless/static-debian12:nonroot

COPY tsm2arc /usr/local/bin/tsm2arc

# The migration mounts InfluxDB data read-only and writes a checkpoint; the
# operator supplies volumes and flags at run time.
ENTRYPOINT ["/usr/local/bin/tsm2arc"]
