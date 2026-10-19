#!/bin/sh
# Make the data directory usable, then run QueueRouter as an unprivileged user.
#
# Started as root (the default): fix ownership of $DATA_DIR if needed (like linuxserver.io
# images, PUID/PGID default to 10001), then permanently drop to PUID:PGID with no capabilities.
# Started as a non-root user (compose `user:`): run directly; the app explains any permission
# problem itself.
set -eu

PUID="${PUID:-10001}"
PGID="${PGID:-10001}"
DATA_DIR="${DATA_DIR:-/data}"

if [ "$(id -u)" != "0" ]; then
    exec "$@"
fi

case "$PUID$PGID" in
    *[!0-9]*) echo "QueueRouter: PUID and PGID must be numbers (got PUID=$PUID PGID=$PGID)" >&2; exit 78 ;;
esac
if [ "$PUID" = "0" ]; then
    echo "QueueRouter: refusing to run as root (PUID=0); pick another PUID" >&2
    exit 78
fi

has_cap() { # capability number -> success if it's in our effective set
    hex=$(sed -n 's/^CapEff:[[:space:]]*//p' /proc/self/status)
    [ $(( (0x$hex >> $1) & 1 )) -eq 1 ]
}

explain_chown_failure() { # $1 = error output; $owner = owner before we touched anything
    fstype=$(awk -v d="$DATA_DIR" '$2 == d {print $3}' /proc/mounts 2>/dev/null | tail -n 1)
    echo "QueueRouter: cannot make $DATA_DIR (owner $owner, filesystem ${fstype:-unknown}) belong to $PUID:$PGID." >&2
    [ -n "$1" ] && printf '%s\n' "$1" | head -n 3 | sed 's/^/  chown: /' >&2
    if ! has_cap 0; then
        echo "  Cause: the container lacks the CHOWN capability." >&2
        echo "  Fix:   add  cap_add: [CHOWN, SETUID, SETGID]  next to  cap_drop: [ALL],  then run" >&2
        echo "         docker compose up -d   (a plain restart does not apply capability changes)" >&2
    else
        echo "  Cause: the filesystem refused the change (typical for NFS/SMB shares and some NAS" >&2
        echo "         paths, or folders the container can't read)." >&2
    fi
    case "$owner" in
        0:*|unknown) echo "  Or:    on the host, sudo chown -R $PUID:$PGID <the folder mounted at $DATA_DIR>" >&2 ;;
        *) echo "  Or:    keep the folder's owner and run as it: set  PUID=${owner%%:*}  PGID=${owner##*:}" >&2 ;;
    esac
    exit 78
}

mkdir -p "$DATA_DIR" 2>/dev/null || true
# Only touch ownership when something is actually wrong (fast on every later start). Children
# go before their parent, so directories stay readable while we work.
owner=$(stat -c '%u:%g' "$DATA_DIR" 2>/dev/null || echo unknown)
if [ -n "$(find "$DATA_DIR" \( ! -user "$PUID" -o ! -group "$PGID" \) -print -quit 2>/dev/null)" ]; then
    # All or nothing: if any part can't even be read, change nothing (a half-changed folder
    # would be worse than the original).
    if ! errors=$(find "$DATA_DIR" -depth 2>&1 >/dev/null) || [ -n "$errors" ]; then
        explain_chown_failure "$errors"
    fi
    if ! errors=$(find "$DATA_DIR" -depth \( ! -user "$PUID" -o ! -group "$PGID" \) \
            -exec chown -h "$PUID:$PGID" {} + 2>&1); then
        explain_chown_failure "$errors"
    fi
    echo "QueueRouter: set owner of $DATA_DIR to $PUID:$PGID (was $owner)" >&2
fi
chmod 700 "$DATA_DIR" 2>/dev/null || true

# Drop to PUID:PGID for good: no supplementary groups. Switching from root to a non-root uid
# clears all effective/permitted capabilities, and no-new-privs stops them coming back.
# (Clearing the bounding set too would need an extra capability, SETPCAP, for no real gain.)
DROP="setpriv --reuid=$PUID --regid=$PGID --clear-groups --inh-caps=-all --no-new-privs"
if ! $DROP -- true 2>/dev/null; then
    echo "QueueRouter: cannot switch to user $PUID:$PGID (missing SETUID/SETGID capabilities)." >&2
    echo "  Fix: add  cap_add: [CHOWN, SETUID, SETGID]  next to  cap_drop: [ALL],  then docker compose up -d" >&2
    echo "  or run it as a fixed user instead:  user: \"$PUID:$PGID\"" >&2
    exit 78
fi
exec $DROP -- "$@"
