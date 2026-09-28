# Solanum (Libera.Chat's ircd, successor of charybdis) for `make test-ircd`.
#
#   docker build -t gobnc-test/solanum:dev docker/ircd/solanum
#   docker run --rm -p 127.0.0.1:4447:4447 gobnc-test/solanum:dev
#
# Solanum publishes no tags or releases, so this pins a commit on the
# default branch (its head on 2026-09-22).

ARG DEBIAN_TAG=trixie-20260918-slim

FROM debian:${DEBIAN_TAG} AS build
ARG SOLANUM_COMMIT=03503ff488ea5757a0fdfd62b7509b02b41904ab

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential pkg-config meson ninja-build flex bison \
      libsqlite3-dev libltdl-dev libssl-dev ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN curl -fsSL "https://github.com/solanum-ircd/solanum/archive/${SOLANUM_COMMIT}.tar.gz" \
      | tar -xz --strip-components=1

# The one source change: authd never loads its reverse-DNS provider, so
# clients register immediately with their IP as host (like bahamut's `n`
# port flag). solanum has no config switch for rDNS, and this host's
# resolver never answers PTR queries for the Docker bridge range, so every
# registration otherwise stalls ~6s -- past the tests' 5s deadline -- and
# the lowest configurable timeout (connect_timeout = 1) still costs 1-2s.
# (authd then logs one harmless "BUG: handle_options got a bad option
# type rdns_timeout" at startup: the ircd still sends that setting.)
# grep -q fails the build if a future pin moves this line.
RUN grep -q '^\s*load_provider(&rdns_provider);' authd/provider.c \
 && sed -i '/^\s*load_provider(&rdns_provider);/d' authd/provider.c

# Built against OpenSSL even though ircd.conf only opens a plain-TCP port:
# librb's no-TLS backend (nossl.c) no longer compiles on current glibc
# (it calls arc4random_stir, which glibc doesn't provide).
RUN meson setup build --prefix=/opt/solanum --optimization=2 \
      -Dopenssl=enabled -Dmbedtls=disabled -Dgnutls=disabled \
 && meson compile -C build \
 && meson install -C build


FROM debian:${DEBIAN_TAG}

# netbase provides /etc/services: authd's resolver looks up the "domain"
# service by name, and without it finds no nameservers and exits in a loop.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libsqlite3-0 libltdl7 libssl3t64 netbase \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home-dir /opt/solanum --shell /usr/sbin/nologin ircd

COPY --from=build /opt/solanum /opt/solanum
COPY ircd.conf /opt/solanum/etc/ircd.conf
COPY entrypoint.sh /entrypoint.sh

# The ircd writes its pid file and ban database under etc/ and var/, and
# its log under logs/.
RUN mkdir -p /opt/solanum/logs /opt/solanum/var \
 && chown -R ircd:ircd /opt/solanum/etc /opt/solanum/var /opt/solanum/logs

USER ircd
EXPOSE 4447
ENTRYPOINT ["/entrypoint.sh"]
