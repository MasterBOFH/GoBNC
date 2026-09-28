# Bahamut (DALnet's ircd) for `make test-ircd`.
#
#   docker build -t gobnc-test/bahamut:dev docker/ircd/bahamut
#   docker run --rm -p 127.0.0.1:4444:4444 gobnc-test/bahamut:dev

ARG DEBIAN_TAG=trixie-20260918-slim

FROM debian:${DEBIAN_TAG} AS build
ARG BAHAMUT_TAG=v2.2.4

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc libc6-dev make autoconf automake zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

ADD https://github.com/DALnet/bahamut/archive/refs/tags/${BAHAMUT_TAG}.tar.gz /bahamut.tar.gz
WORKDIR /src
RUN tar -xzf /bahamut.tar.gz --strip-components=1

# The one source change: compile out connection throttling (and with it
# per-IP clone limiting, which the same define gates). Every test client
# comes from one address (the Docker bridge gateway), and the throttle
# fires at accept time -- 3 connections in 15s earns a 2-minute, then
# escalating, z:line -- before the allow block's T flag can exempt
# anyone. There is no config-file switch for it, only this compile-time
# define in config.h or an oper's runtime SET THROTTLE.
# grep -q fails the build if a future tag moves this line.
RUN grep -q '^#define THROTTLE_ENABLE' include/config.h \
 && sed -i 's/^#define THROTTLE_ENABLE/#undef THROTTLE_ENABLE/' include/config.h

# The repo ships only configure.in, so generate include/setup.h.in and
# configure first (with automake's copies of the auxiliary scripts
# configure expects alongside).
# --with-maxconnections: MAXCONNECTIONS is compile-time and sizes static
# arrays; left on "auto" it is probed from the *build* container's
# RLIMIT_NOFILE (Docker's default on this host is 1073741816). At runtime
# the ircd lowers its own soft limit to exactly this value, so no
# entrypoint ulimit is needed.
# --disable-openssl: the test listener is plain TCP only.
RUN for f in install-sh config.guess config.sub; do \
      cp /usr/share/automake-*/$f .; \
    done \
 && autoheader \
 && autoconf \
 && ./configure --prefix=/opt/bahamut --with-maxconnections=1024 --disable-openssl \
 && make \
 && make install


FROM debian:${DEBIAN_TAG}

RUN apt-get update \
 && apt-get install -y --no-install-recommends zlib1g \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home-dir /opt/bahamut --shell /usr/sbin/nologin ircd

COPY --from=build /opt/bahamut /opt/bahamut
COPY ircd.conf /opt/bahamut/ircd.conf

# The ircd writes its pid file, .maxclients and k-line journal next to
# ircd.conf.
RUN chown -R ircd:ircd /opt/bahamut

USER ircd
WORKDIR /opt/bahamut
EXPOSE 4444
# -t: stay in the foreground (don't fork/detach) as the container's PID 1.
ENTRYPOINT ["/opt/bahamut/ircd", "-t"]
