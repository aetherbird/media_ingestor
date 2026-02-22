#!/usr/bin/env bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
set -euo pipefail

############################
# CONFIG
############################

INCOMING="/ssdtemp/incoming"
QUEUE_ROOT="/ssdtemp/.ingest-queue"
VIDEOS_ROOT="/herd/family/videos"
MISC_ROOT="/herd/family/misc"
LOGFILE="/var/log/ingest-media.log"

STABILITY_SECONDS=30
BIGFILE_BYTES=$((3 * 1024 * 1024 * 1024)) # 3GB

LOCKFILE="/run/lock/ingest-media.lock"

############################
# HELPERS
############################

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOGFILE"
}

is_open_for_write() {
    # best-effort
    lsof "$1" 2>/dev/null | grep -q "$1" && return 0 || return 1
}

is_stable() {
    local f="$1"

    # mtime check
    local now
    now=$(date +%s)
    local mtime
    mtime=$(stat -c %Y "$f")
    if (( now - mtime < STABILITY_SECONDS )); then
        return 1
    fi

    # open check
    if is_open_for_write "$f"; then
        return 1
    fi

    # size double sample
    local s1 s2
    s1=$(stat -c %s "$f")
    sleep 2
    s2=$(stat -c %s "$f")

    [[ "$s1" -eq "$s2" ]]
}

has_video_stream() {
    ffprobe -v error -select_streams v \
        -show_entries stream=codec_type \
        -of csv=p=0 "$1" 2>/dev/null | grep -q video
}

probe_sanity() {
    timeout 10 ffprobe -v error "$1" >/dev/null 2>&1
}

copy_then_remove() {
    local src="$1"
    local dest="$2"

    mkdir -p "$(dirname "$dest")"

    if rsync -a --ignore-existing "$src" "$dest"; then
        rm -f "$src"
        return 0
    else
        return 1
    fi
}

############################
# MAIN
############################

exec 9>"$LOCKFILE"
flock -n 9 || exit 0

RUNID=$(date '+%Y-%m-%d-%H%M%S')
QUEUE="$QUEUE_ROOT/$RUNID"
mkdir -p "$QUEUE"

############################
# CLAIM READY FILES
############################

CLAIM_COUNT=0

while IFS= read -r -d '' file; do
    if is_stable "$file"; then
        rel="${file#$INCOMING/}"
        dest="$QUEUE/$rel"
        mkdir -p "$(dirname "$dest")"
        mv "$file" "$dest"
        CLAIM_COUNT=$((CLAIM_COUNT + 1))
    fi
done < <(find "$INCOMING" -type f -print0)

# If nothing claimed, exit silently (and remove empty queue dir)
if (( CLAIM_COUNT == 0 )); then
    rmdir "$QUEUE" 2>/dev/null || true
    exit 0
fi

log "==== RUN START ($CLAIM_COUNT files claimed) ===="

############################
# BEETS PASS
############################

log "Running beets..."
beet import -q "$QUEUE" >> "$LOGFILE" 2>&1 || true

############################
# VIDEO PASS
############################

TIMESTAMP_DIR=$(date '+%Y-%m-%d-%H')
VIDEO_BUCKET="$VIDEOS_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d '' file; do
    rel="${file#$QUEUE/}"
    ext="${file##*.}"
    ext_lower=$(echo "$ext" | tr '[:upper:]' '[:lower:]')
    size=$(stat -c %s "$file")

    is_video=0

    case "$ext_lower" in
        mkv|avi|mov|webm|ts|m2ts|mpg|mpeg|wmv)
            is_video=1
            ;;
        mp4|m4v)
            if has_video_stream "$file"; then
                is_video=1
            fi
            ;;
        *)
            mime=$(file -b --mime-type "$file")
            if [[ "$mime" == video/* ]]; then
                is_video=1
            fi
            ;;
    esac

    if (( is_video == 1 )); then
        # optional big file sanity check
        if (( size > BIGFILE_BYTES )); then
            if ! probe_sanity "$file"; then
                log "Sanity probe failed: $rel"
                continue
            fi
        fi

        # determine loose vs tree
        if [[ "$rel" == */* ]]; then
            dest="$VIDEOS_ROOT/$rel"
        else
            dest="$VIDEO_BUCKET/$rel"
        fi

        if copy_then_remove "$file" "$dest"; then
            log "VIDEO -> $dest"
        fi
    fi
done < <(find "$QUEUE" -type f -print0)

############################
# MISC PASS (only leftovers)
############################

while IFS= read -r -d '' file; do
    rel="${file#$QUEUE/}"
    dest="$MISC_ROOT/$TIMESTAMP_DIR/$rel"

    if copy_then_remove "$file" "$dest"; then
        log "MISC -> $dest"
    fi
done < <(find "$QUEUE" -type f -print0)

############################
# CLEANUP
############################

find "$QUEUE" -type d -empty -delete
rmdir "$QUEUE" 2>/dev/null || true

log "==== RUN END ===="
