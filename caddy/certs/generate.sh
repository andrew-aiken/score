#!/usr/bin/env bash
# Regenerates the test self-signed TLS certificate & key for Caddy

set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -keyout localhost.key -out localhost.crt -days 3650 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
