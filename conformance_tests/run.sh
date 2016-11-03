#!/bin/bash
set -eu

SELFDIR=$(dirname "$0")
FRAMEQUERY_ENV=$1 python3 -m pytest "${SELFDIR}"
