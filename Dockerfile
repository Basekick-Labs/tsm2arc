# tsm2arc is built by GoReleaser (static, CGO_ENABLED=0); this image just
# packages the prebuilt binary. Distroless static gives a tiny, CVE-light image
# with a CA bundle (needed for HTTPS to Arc) and no shell/package manager.
#
# GoReleaser dockers_v2 stages the prebuilt binaries into the build context by
# platform (e.g. linux/amd64/tsm2arc), so we COPY from $TARGETPLATFORM rather
# than a flat path.
FROM gcr.io/distroless/static-debian12:nonroot

ARG TARGETPLATFORM
COPY ${TARGETPLATFORM}/tsm2arc /usr/local/bin/tsm2arc

# The migration mounts InfluxDB data read-only and writes a checkpoint; the
# operator supplies volumes and flags at run time.
ENTRYPOINT ["/usr/local/bin/tsm2arc"]
