#!/bin/sh
set -eu
# Bind shellctl to the container's primary IP on isolated per-sandbox networks.
if [ -z "${SHELLCTL_HOST:-}" ] || [ "${SHELLCTL_HOST}" = "auto" ]; then
  export SHELLCTL_HOST="0.0.0.0"
fi
exec /usr/local/bin/shellctl "$@"
