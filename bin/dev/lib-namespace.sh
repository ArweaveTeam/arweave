# shellcheck shell=bash
#
# lib-namespace.sh — shared helper for Arweave's core launcher scripts.
#
# SOURCE this file (do not execute it). If ARWEAVE_NAMESPACE is set it is
# validated and ARWEAVE_NS_SUFFIX is exported as "-<namespace>". Callers append
# that suffix to Erlang node names so several checkouts can run side by side on
# one host without colliding on EPMD / node names. When ARWEAVE_NAMESPACE is
# unset the suffix is empty and behaviour is unchanged.
#
# See doc/workspaces.md and bin/dev/ws.

case "${ARWEAVE_NAMESPACE:-}" in
    "")
        ARWEAVE_NS_SUFFIX=""
        ;;
    *[!a-zA-Z0-9._-]*)
        echo "Arweave: invalid ARWEAVE_NAMESPACE '${ARWEAVE_NAMESPACE}'" \
             "(allowed characters: a-z A-Z 0-9 . _ -)" >&2
        exit 1
        ;;
    *)
        ARWEAVE_NS_SUFFIX="-${ARWEAVE_NAMESPACE}"
        ;;
esac
export ARWEAVE_NS_SUFFIX
