#!/usr/bin/env bash
# Regenerates the test self-signed TLS certificate & key for Caddy

set -euo pipefail
cd "$(dirname "$0")"

openssl ecparam -name prime256v1 -genkey -noout -out localhost.key

openssl req -new -x509 -key localhost.key -out localhost.crt -days 3650 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
