# ircd-irc2 (IRCnet's ircd 2.11) built from a pinned upstream tag, for
# GoBNC's `make test-ircd` interop matrix.
ARG DEBIAN_TAG=trixie-slim

FROM debian:${DEBIAN_TAG} AS build
ARG IRCD_TAG=v2.11.3
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates git build-essential \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${IRCD_TAG}" \
      https://github.com/ircnet/ircd /src
WORKDIR /src
# This ircd has no switch for reverse DNS on clients. It is not a
# connect-back, but it still made each registration wait on the
# host's resolver (2s seen once here; ~28s with DNS unreachable).
# The patch skips the lookup; clients show by IP, as on DNS failure.
COPY no-reverse-dns.patch /tmp/
RUN git apply /tmp/no-reverse-dns.patch
# Compile-time settings (./configure copies config.h.dist to config.h).
# No connect-back checks (see ircd.conf for why):
#   USE_IAUTH off: no iauth process, so no ident/SOCKS/webproxy/DNSBL.
#   NO_IDENT on: ircd then won't do ident itself either.
# LISTENER_DELAY off: by default the listener is polled only once a
#   second, so a client connecting just after another waits ~1-2s to
#   be accepted.
# The greps fail the build if upstream renamed any of these lines.
RUN sed -i -e 's|^#define USE_IAUTH$|#undef USE_IAUTH|' \
      -e 's|^/\* #undef\tNO_IDENT \*/$|#define NO_IDENT|' \
      -e 's|^#define LISTENER_DELAY 1$|#undef LISTENER_DELAY|' \
      support/config.h.dist \
 && grep -q '^#undef USE_IAUTH$' support/config.h.dist \
 && grep -q '^#define NO_IDENT$' support/config.h.dist \
 && grep -q '^#undef LISTENER_DELAY$' support/config.h.dist
RUN ./configure --prefix=/opt/ircd --sysconfdir=/opt/ircd/etc \
      --with-rundir=/opt/ircd/run --with-logdir=/opt/ircd/log \
      --without-zlib \
 && make -C "$(support/config.guess)" ircd \
 && install -D -m 755 "$(support/config.guess)/ircd" /opt/ircd/sbin/ircd

FROM debian:${DEBIAN_TAG}
RUN useradd --system --home-dir /opt/ircd --shell /usr/sbin/nologin ircd
COPY --from=build /opt/ircd/sbin/ircd /opt/ircd/sbin/ircd
COPY --from=build /src/support/ircd.motd /opt/ircd/etc/ircd.motd
COPY ircd.conf /opt/ircd/etc/ircd.conf
# ircd writes its pid/tune file and logs here.
RUN install -d -o ircd -g ircd /opt/ircd/run /opt/ircd/log
USER ircd
EXPOSE 4440
# -t: stay in the foreground as PID 1.
# -p standalone: a lone server otherwise sits in split mode forever
#   (thresholds are floored at 10 servers / 5000 users at compile
#   time), and in split mode a channel's first joiner gets no ops.
CMD ["/opt/ircd/sbin/ircd", "-t", "-p", "standalone"]
