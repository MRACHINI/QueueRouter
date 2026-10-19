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

mkdir -p "$DATA_DIR" 2>/dev/null || true
# Only touch ownership when something is actually wrong (fast on every later start).
if [ -n "$(find "$DATA_DIR" \( ! -user "$PUID" -o ! -group "$PGID" \) -print -quit 2>/dev/null)" ]; then
    if ! chown -R "$PUID:$PGID" "$DATA_DIR" 2>/dev/null; then
        echo "QueueRouter: cannot change the owner of $DATA_DIR to $PUID:$PGID." >&2
        echo "  Add to the service:  cap_add: [CHOWN, SETUID, SETGID]   (keep cap_drop: [ALL])" >&2
        echo "  or fix it on the host:  sudo chown -R $PUID:$PGID /path/on/host" >&2
        exit 78
    fi
    echo "QueueRouter: set owner of $DATA_DIR to $PUID:$PGID" >&2
fi
chmod 700 "$DATA_DIR" 2>/dev/null || true

# Drop to PUID:PGID for good: no supplementary groups. Switching from root to a non-root uid
# clears all effective/permitted capabilities, and no-new-privs stops them coming back.
# (Clearing the bounding set too would need an extra capability, SETPCAP, for no real gain.)
DROP="setpriv --reuid=$PUID --regid=$PGID --clear-groups --inh-caps=-all --no-new-privs"
if ! $DROP -- true 2>/dev/null; then
    echo "QueueRouter: cannot switch to user $PUID:$PGID (missing capabilities)." >&2
    echo "  Add to the service:  cap_add: [CHOWN, SETUID, SETGID]   (keep cap_drop: [ALL])" >&2
    echo "  or run it as a fixed user instead:  user: \"$PUID:$PGID\"" >&2
    exit 78
fi
exec $DROP -- "$@"
