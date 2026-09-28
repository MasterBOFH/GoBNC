# ircd-ratbox, built from the pinned final upstream release, for `make test-ircd`.
#
#   docker build -t gobnc-test/ratbox:dev docker/ircd/ratbox
#   docker run --rm -p 127.0.0.1:4446:4446 gobnc-test/ratbox:dev
#
# Source: 3.0.10 (Jan 2016) is the last release on the official download
# site. The github.com/ircd-ratbox/ircd-ratbox repository is empty (no
# commits at all), so there is no git ref to pin instead.

ARG DEBIAN_TAG=trixie-slim

FROM debian:${DEBIAN_TAG} AS build
ARG RATBOX_VERSION=3.0.10
# Published upstream as ircd-ratbox-3.0.10.tar.bz2.sha256.
ARG RATBOX_SHA256=8d871ab81e8581179c3b831e25bd2f6d4f6dfc77380cb190237221bda247ead4
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential bzip2 ca-certificates curl patch \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /ratbox.tar.bz2 \
      "https://www.ratbox.org/download/ircd-ratbox-${RATBOX_VERSION}.tar.bz2" \
 && echo "${RATBOX_SHA256}  /ratbox.tar.bz2" | sha256sum -c - \
 && mkdir /src && tar -xjf /ratbox.tar.bz2 -C /src --strip-components=1
WORKDIR /src
# ratbox has no config switch to skip reverse DNS of connecting clients (and
# this host's DNS server can take >10s to NXDOMAIN a cold PTR query for the
# Docker gateway, past the tests' 5s registration deadline), so patch it out.
COPY no-reverse-dns.patch /
RUN patch -p1 < /no-reverse-dns.patch
# glibc >= 2.36 has arc4random() but not arc4random_stir(), which libratbox's
# no-TLS PRNG calls whenever configure finds arc4random; force configure to
# use libratbox's own bundled arc4random implementation instead.
RUN ac_cv_func_arc4random=no \
    ./configure --prefix=/opt/ratbox --disable-openssl --disable-zlib \
 && make \
 && make install

FROM debian:${DEBIAN_TAG}
# netbase provides /etc/services. ratbox's resolver helper resolves each
# resolv.conf nameserver with getaddrinfo(ns, "domain"); without that file
# every nameserver is silently dropped and no DNS query is ever sent. (Client
# lookups are patched out above; without that patch, this alone made every
# client stall ~30s at "Looking up your hostname".)
RUN apt-get update \
 && apt-get install -y --no-install-recommends netbase \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home-dir /opt/ratbox --shell /usr/sbin/nologin ircd
COPY --from=build /opt/ratbox /opt/ratbox
COPY ircd.conf /opt/ratbox/etc/ircd.conf
# The ircd writes its pid file and ban database under etc/, logs under logs/.
RUN mkdir -p /opt/ratbox/logs \
 && chown -R ircd:ircd /opt/ratbox/etc /opt/ratbox/logs
USER ircd
WORKDIR /opt/ratbox
EXPOSE 4446
# libratbox spins at 100% CPU and never answers a client when RLIMIT_NOFILE
# is as huge as Docker's default can be (1073741816), so cap it here rather
# than rely on the runner passing --ulimit.
CMD ["/bin/sh", "-c", "ulimit -n 4096 && exec /opt/ratbox/bin/ircd -foreground"]
