#!/usr/bin/env bash
# Полный прогон MapReduce: clean -> map -> reduce -> collect
set -euo pipefail
cd "$(dirname "$0")"
NS=${NAMESPACE:-hh}
for s in clean map reduce collect; do
  echo "=== $s ==="
  ./render.sh "$s"
  kubectl -n "$NS" wait --for=condition=complete "job/mr-$s" --timeout=30m
done
kubectl -n "$NS" logs job/mr-collect