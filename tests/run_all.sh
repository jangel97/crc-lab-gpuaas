#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== VK core tests (single tenant, 14GB) ==="
python3 -m pytest -m vk -v "$@"

echo "=== RHOAI tests (single tenant, 28GB) ==="
python3 -m pytest -m rhoai -v "$@"

echo "=== Kueue multi-tenant tests (dual tenant, 14GB) ==="
python3 -m pytest -m kueue -v "$@"

echo "=== Networking tests (single tenant, 14GB) ==="
python3 -m pytest -m networking -v "$@"
