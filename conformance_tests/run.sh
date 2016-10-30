#!/bin/bash
set -eu

SELFDIR=$(dirname "$0")
CONFIG="$1"
shift

FRAMEQUERY_ENV="$CONFIG" python3 -m pytest "${SELFDIR}" "$@"
