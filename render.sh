#!/usr/bin/env bash
# Использование: ./render.sh single|clean|map|reduce|collect|filler
set -euo pipefail
cd "$(dirname "$0")/k8s"
STAGE=${1:?usage: render.sh single|clean|map|reduce|collect|filler}
export STAGE NAMESPACE=${NAMESPACE:-hh} IMAGE=${IMAGE:-hh-mr:1.0} K=${K:-2}
export REDUCERS=${REDUCERS:-2} PRIORITY_CLASS= HOLD_SECONDS=0
KIND=job

case "$STAGE" in
  single)
    TEMPLATE=job-single.yaml; export NAME=pairs-single
    export MEM_LIMIT=${MEM_LIMIT:-1Gi} ;;
  filler)
    TEMPLATE=filler.yaml; KIND=deploy; export NAME=filler-low
    export FILL_REPLICAS=${FILL_REPLICAS:-1} FILL_CPU=${FILL_CPU:-6} ;;
  clean|map|reduce|collect)
    TEMPLATE=job-mr.yaml; export NAME=mr-$STAGE
    export MEM_LIMIT=${MEM_LIMIT:-512Mi}
    export COMPLETIONS=8 PARALLELISM=2 CPU_REQUEST=1
    if [ "$STAGE" = map ]; then
      export COMPLETIONS=${COMPLETIONS_MAP:-8} PARALLELISM=${PARALLELISM_MAP:-4}
      export CPU_REQUEST=${CPU_REQUEST:-500m} HOLD_SECONDS=${HOLD_SECONDS_MAP:-50}
      export PRIORITY_CLASS=${PRIORITY_CLASS_MAP:-}
    elif [ "$STAGE" = reduce ]; then
      export COMPLETIONS=$REDUCERS PARALLELISM=$REDUCERS
    fi ;;
  *) echo "неизвестная стадия $STAGE"; exit 1 ;;
esac

if [ "$KIND" = job ]; then
  kubectl -n "$NAMESPACE" delete job "$NAME" --ignore-not-found --wait=true
fi
envsubst < "$TEMPLATE" | kubectl apply -f -