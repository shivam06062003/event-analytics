#!/usr/bin/env bash
# Local Kubernetes environment: kind cluster + metrics-server + KEDA + backing
# services + this app via its Helm chart. Idempotent: safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=analytics
NS=analytics
KEDA_VERSION=2.21.0
METRICS_SERVER_VERSION=3.14.0

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

step "Cluster"
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --config deploy/k8s/kind-config.yaml --wait 120s
kubectl config use-context "kind-$CLUSTER" >/dev/null

step "metrics-server (CPU metrics for the API's HorizontalPodAutoscaler)"
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null 2>&1 || true
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update >/dev/null
# kind's kubelets use self-signed certificates.
helm upgrade --install metrics-server metrics-server/metrics-server --version "$METRICS_SERVER_VERSION" \
  -n kube-system --set 'args={--kubelet-insecure-tls}' --wait

step "KEDA (event-driven autoscaling on Kafka lag)"
helm upgrade --install keda kedacore/keda --version "$KEDA_VERSION" -n keda --create-namespace --wait

step "Backing services (local-only manifests)"
kubectl apply -f deploy/k8s/infra/stateful-services.yaml
kubectl -n "$NS" rollout status statefulset/postgres statefulset/redpanda statefulset/clickhouse --timeout=300s
kubectl -n "$NS" rollout status deployment/redis --timeout=120s

step "Build the app image and load it into the cluster (no registry needed)"
docker build -q -t event-analytics:dev . >/dev/null
kind load docker-image event-analytics:dev --name "$CLUSTER"

step "Install/upgrade the app (migrations run as a pre-install hook)"
kubectl -n "$NS" create configmap loadtest-scripts --from-file=loadtest/ingest.js \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm upgrade --install analytics deploy/helm/event-analytics -n "$NS" \
  -f deploy/k8s/values-kind.yaml --wait --timeout 5m

kubectl -n "$NS" get pods,scaledobject,hpa
