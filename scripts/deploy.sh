#!/usr/bin/env sh
# Deploy the notifier service as a Docker image.
#
# Usage: ./scripts/deploy.sh [env]
#   env: target environment (default: local)
#        local — build the image and run the full stack on Docker Desktop
#        test/prod — reserved for future environments
set -eu

# Anchor to the repo root so the script works from any directory.
cd "$(dirname "$0")/.."

IMAGE_NAME="notifier"
# Single source for the local API address; override with API_BASE=...
API_BASE="${API_BASE:-http://localhost:8081}"
LOCAL_HEALTH_URL="${API_BASE}/healthz"
HEALTH_RETRIES=30

TARGET_ENV="${1:-local}"

fail() {
    echo "deploy: $1" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker not found in PATH"
docker info >/dev/null 2>&1 || fail "docker daemon is not running (start Docker Desktop)"

deploy_local() {
    GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo dev)"

    echo "==> Building ${IMAGE_NAME}:local (${GIT_SHA})"
    docker build -t "${IMAGE_NAME}:local" -t "${IMAGE_NAME}:${GIT_SHA}" .

    echo "==> Starting stack"
    docker compose up -d

    echo "==> Waiting for API health"
    attempt=1
    while [ "$attempt" -le "$HEALTH_RETRIES" ]; do
        if curl -fsS -o /dev/null "$LOCAL_HEALTH_URL" 2>/dev/null; then
            echo "==> Deployed: API healthy at ${LOCAL_HEALTH_URL} (image ${IMAGE_NAME}:${GIT_SHA})"
            docker compose ps
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    fail "API did not become healthy within ${HEALTH_RETRIES}s — check: docker compose logs api"
}

case "$TARGET_ENV" in
    local)
        deploy_local
        ;;
    test|prod)
        fail "environment '${TARGET_ENV}' is not implemented yet"
        ;;
    *)
        fail "unknown environment '${TARGET_ENV}' (supported: local)"
        ;;
esac
