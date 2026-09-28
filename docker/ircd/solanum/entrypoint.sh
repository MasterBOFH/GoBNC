#!/bin/sh
# librb sizes its fd tables from RLIMIT_NOFILE and walks all of them, so
# under Docker's default limit on this host (1073741816) every solanum
# process (ircd, authd, bandb, ...) spins at 100% CPU and never answers a
# client. Cap it here so the image works without a compose `ulimits:`.
ulimit -n 4096 || exit 1
exec /opt/solanum/bin/solanum -foreground "$@"
