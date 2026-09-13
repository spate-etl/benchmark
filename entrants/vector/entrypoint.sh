#!/bin/sh
# Selects the config for this source count.
#
# FORMAT is checked rather than used: the configs hardcode json_each_row, and a
# run that asked for another format would otherwise measure something nobody
# asked for while the record named the format it requested.
set -eu

if [ "${FORMAT:-json_each_row}" != json_each_row ]; then
    echo "FORMAT must be json_each_row, got '${FORMAT:-}'" >&2
    exit 1
fi

config=/etc/vector/vector-s${SOURCES:-8}.yaml
if [ ! -f "$config" ]; then
    echo "SOURCES must be one of 1 2 4 8 16 32, got '${SOURCES:-}'" >&2
    exit 1
fi

exec vector --config "$config"
