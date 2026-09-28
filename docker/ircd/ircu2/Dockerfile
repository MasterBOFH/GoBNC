# ircu2 (Undernet) built from a pinned upstream tag, for GoBNC's
# `make test-ircd` interop matrix.
ARG DEBIAN_TAG=trixie-slim

FROM debian:${DEBIAN_TAG} AS build
ARG IRCU_TAG=u2.10.12.19
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates git build-essential flex bison \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${IRCU_TAG}" \
      https://github.com/UndernetIRC/ircu2 /src
WORKDIR /src
# --with-maxcon: configure otherwise takes `ulimit -Hn` of the build
#   container (~1e9 under Docker) and sizes its connection tables by it.
# --with-domain: else read from the build host's resolv.conf (not reproducible).
RUN ./configure --prefix=/opt/ircu --with-dpath=/opt/ircu/lib \
      --with-maxcon=1024 --with-domain=test \
 && make \
 && install -D -m 755 ircd/ircd /opt/ircu/bin/ircd

FROM debian:${DEBIAN_TAG}
RUN useradd --system --home-dir /opt/ircu/lib --shell /usr/sbin/nologin ircd
COPY --from=build /opt/ircu/bin/ircd /opt/ircu/bin/ircd
COPY ircd.conf /opt/ircu/lib/ircd.conf
# ircd writes its pid and log files into its data dir.
RUN touch /opt/ircu/lib/ircd.motd \
 && chown -R ircd:ircd /opt/ircu/lib
USER ircd
WORKDIR /opt/ircu/lib
EXPOSE 4443
# -n: stay in the foreground as PID 1.
CMD ["/opt/ircu/bin/ircd", "-n", "-f", "/opt/ircu/lib/ircd.conf"]
