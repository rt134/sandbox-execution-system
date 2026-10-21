#!/bin/sh
set -eu
# Bind shellctl to the container's primary IP on isolated per-sandbox networks.
if [ -z "${SHELLCTL_HOST:-}" ] || [ "${SHELLCTL_HOST}" = "auto" ]; then
  ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  if [ -n "$ip" ]; then
    export SHELLCTL_HOST="$ip"
  else
    export SHELLCTL_HOST="0.0.0.0"
  fi
fi
exec /usr/local/bin/shellctl "$@"
