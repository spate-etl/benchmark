#!/bin/sh
# Selects the config for this variant's wire format and source count.
#
# Each config hardcodes its own `format` and carries exactly the encoder that
# format needs, so the pairing cannot be got wrong by an environment variable:
# FORMAT chooses a file, not a field. An unknown value fails here rather than
# starting an arm that would measure something nobody asked for.
set -eu

case "${FORMAT:-json_each_row}" in
    arrow_stream)  fmt=arrow ;;
    json_each_row) fmt=json ;;
    *)
        echo "FORMAT must be arrow_stream or json_each_row, got '${FORMAT:-}'" >&2
        exit 1
        ;;
esac

config=/etc/vector/vector-$fmt-s${SOURCES:-8}.yaml
if [ ! -f "$config" ]; then
    echo "SOURCES must be one of 1 2 4 8 16 32, got '${SOURCES:-}'" >&2
    exit 1
fi

# TRANSFORM is interpolated into the config's `file:` path, so a bad value would
# surface as a missing-file error from Vector; name it here instead.
if [ ! -f "/etc/vector/transform-${TRANSFORM:-fused}.vrl" ]; then
    echo "TRANSFORM must be fused or twopass, got '${TRANSFORM:-}'" >&2
    exit 1
fi

exec vector --config "$config"
