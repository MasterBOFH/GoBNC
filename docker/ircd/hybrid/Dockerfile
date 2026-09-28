# ircd-hybrid, built from a pinned upstream tag, for `make test-ircd`.
#
#   docker build -t gobnc-test/hybrid:dev docker/ircd/hybrid
#   docker run --rm -p 127.0.0.1:4442:4442 gobnc-test/hybrid:dev

ARG DEBIAN_TAG=trixie-slim

FROM debian:${DEBIAN_TAG} AS build
ARG HYBRID_TAG=8.2.47
# Commit the tag points at; the build fails if the tag is ever moved.
ARG HYBRID_COMMIT=1ad473137191b1a7e2992145dcf67bf6f44beb7c
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential ca-certificates git libjansson-dev \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${HYBRID_TAG}" \
      https://github.com/ircd-hybrid/ircd-hybrid /src \
 && test "$(git -C /src rev-parse HEAD)" = "${HYBRID_COMMIT}"
WORKDIR /src
# Plain-text only: the interop suite doesn't use TLS against this server.
RUN ./configure --prefix=/opt/hybrid --with-tls=none \
 && make -j"$(nproc)" \
 && make install

FROM debian:${DEBIAN_TAG}
RUN apt-get update \
 && apt-get install -y --no-install-recommends libjansson4 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --home-dir /opt/hybrid --shell /usr/sbin/nologin ircd
COPY --from=build /opt/hybrid /opt/hybrid
COPY ircd.conf /opt/hybrid/etc/ircd.conf
# The ircd writes its pid file, logs and k/d/x-line databases under var/.
RUN chown -R ircd:ircd /opt/hybrid/var
USER ircd
WORKDIR /opt/hybrid
EXPOSE 4442
CMD ["/opt/hybrid/bin/ircd", "-foreground"]
