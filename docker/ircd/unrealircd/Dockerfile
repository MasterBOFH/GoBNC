# UnrealIRCd 6, built from the pinned official release tarball, for GoBNC's
# `make test-ircd` interop matrix. Plaintext client port: 4441.
#
#   docker build -t gobnc-test/unrealircd:dev docker/ircd/unrealircd
#   docker run --rm -p 127.0.0.1:4441:4441 gobnc-test/unrealircd:dev

ARG DEBIAN_IMAGE=debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a

FROM ${DEBIAN_IMAGE} AS build

# UnrealIRCd doesn't publish git tags; this is the release tarball from
# unrealircd.org. SHA256 matches the one published on
# https://www.unrealircd.org/download/6.0/6.2.7/src (the tarball's .asc GPG
# signature also verifies against the UnrealIRCd release key).
ARG VERSION=6.2.7
ARG SHA256=e075375f469309bf1d54134bc914ec50a4ae3b09da6d2db2c11f9fa88d649370

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential pkg-config ca-certificates curl openssl \
      libssl-dev libpcre2-dev libargon2-dev libsodium-dev libc-ares-dev libjansson-dev \
 && rm -rf /var/lib/apt/lists/*

# UnrealIRCd refuses to build or run as root.
RUN useradd --create-home --home-dir /home/ircd --uid 10001 --shell /bin/sh ircd
USER ircd
WORKDIR /home/ircd

RUN curl -fsSL -o unrealircd.tar.gz "https://www.unrealircd.org/downloads/unrealircd-${VERSION}.tar.gz" \
 && echo "${SHA256}  unrealircd.tar.gz" | sha256sum -c - \
 && tar -xzf unrealircd.tar.gz \
 && rm unrealircd.tar.gz

WORKDIR /home/ircd/unrealircd-${VERSION}
# ./Config is interactive; `-quick` skips every question and runs
# ./configure straight from the answers in config.settings.
COPY --chown=ircd:ircd config.settings ./config.settings
RUN ./Config -quick \
 && make -j"$(nproc)" \
 && make install

# UnrealIRCd 6 loads set::tls::certificate/key at boot even with only a
# plaintext listener (also used for STARTTLS); self-signed is fine here.
RUN mkdir -p /home/ircd/unrealircd/conf/tls \
 && openssl req -x509 -nodes -newkey ec -pkeyopt ec_paramgen_curve:secp384r1 \
      -days 3650 -subj "/CN=unrealircd.test" \
      -keyout /home/ircd/unrealircd/conf/tls/server.key.pem \
      -out /home/ircd/unrealircd/conf/tls/server.cert.pem

COPY --chown=ircd:ircd unrealircd.conf /home/ircd/unrealircd/conf/unrealircd.conf
# Fail the build on a bad config rather than at container start.
RUN /home/ircd/unrealircd/unrealircd configtest

FROM ${DEBIAN_IMAGE}

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libssl3t64 libpcre2-8-0 libargon2-1 libsodium23 libc-ares2 libjansson4 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --home-dir /home/ircd --uid 10001 --shell /bin/sh ircd

# Same absolute path as the build stage: paths (and the private lib rpath)
# are compiled in.
COPY --from=build --chown=ircd:ircd /home/ircd/unrealircd /home/ircd/unrealircd

USER ircd
EXPOSE 4441
# -F: stay in the foreground as PID 1; logs go to stderr (docker logs).
ENTRYPOINT ["/home/ircd/unrealircd/bin/unrealircd", "-F"]
