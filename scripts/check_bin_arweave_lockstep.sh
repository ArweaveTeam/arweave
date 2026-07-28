#!/usr/bin/env bash
# Fail when bin/arweave has drifted from priv/templates/extended_bin.
#
# bin/arweave is the committed, pre-rendered copy of the relx overlay
# template priv/templates/extended_bin: identical text with every
# {{ var }} / {{{ var }}} rendered empty (the launcher treats empty
# release variables as "running from sources"). Any launcher change
# must therefore land in BOTH files; this check renders the template
# with empty variables and requires a byte-exact match.
set -euo pipefail
cd "$(dirname "$0")/.."

TEMPLATE=priv/templates/extended_bin
RENDERED=$(mktemp)
trap 'rm -f "$RENDERED"' EXIT

# The real renderer is relx/bbmustache at release-build time. For THIS
# check the wanted context is "every variable empty", and for simple
# variable tags — {{ var }} and {{{ var }}}, the only mustache
# constructs this template uses — deleting the tag is exactly
# bbmustache's empty-context output. Guard that subset assumption:
# sections, inverted sections, comments, or partials would render
# differently, so fail loudly if one ever appears.
if grep -nE '\{\{[#/^!>&]' "$TEMPLATE"; then
    echo "" 1>&2
    echo "error: ${TEMPLATE} uses a mustache construct beyond simple" 1>&2
    echo "variable tags; teach this check to render it first." 1>&2
    exit 1
fi

sed -E 's/\{\{\{ *[a-zA-Z0-9_]+ *\}\}\}//g; s/\{\{ *[a-zA-Z0-9_]+ *\}\}//g' \
    "$TEMPLATE" > "$RENDERED"

if diff -u "$RENDERED" bin/arweave; then
    echo "OK: bin/arweave is in lockstep with ${TEMPLATE}"
else
    echo "" 1>&2
    echo "error: bin/arweave and ${TEMPLATE} have drifted." 1>&2
    echo "Apply the change to BOTH files: bin/arweave must equal the" 1>&2
    echo "template with every {{ var }} rendered empty." 1>&2
    exit 1
fi
