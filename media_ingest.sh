#!/usr/bin/env bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
set -euo pipefail

############################
# CONFIG
############################

INCOMING="/ssdtemp/incoming"
QUEUE_ROOT="/ssdtemp/.ingest-queue"
VIDEOS_ROOT="/herd/family/videos"
MUSIC_UNMATCHED_ROOT="/herd/family/music/unmatched"
MISC_ROOT="/herd/family/misc"
LOGFILE="/var/log/ingest-media.log"

STABILITY_SECONDS=44
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
    sleep 3
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

    # If destination already exists and matches size, treat as duplicate and delete source.
    if [[ -e "$dest" ]]; then
        local s_src s_dst
        s_src=$(stat -c %s "$src" 2>/dev/null || echo 0)
        s_dst=$(stat -c %s "$dest" 2>/dev/null || echo -1)
        if [[ "$s_src" -eq "$s_dst" && "$s_src" -gt 0 ]]; then
            rm -f "$src"
            return 0
        fi
        # dest exists but doesn't match; do not delete source
        return 1
    fi

    # Copy and delete on success
    if rsync -a "$src" "$dest"; then
        rm -f "$src"
        return 0
    fi

    return 1
}

############################
# MAIN
############################

exec 9>"$LOCKFILE"
flock -n 9 || exit 0

RUNID=$(date '+%Y-%m-%d-%H%M%S')
QUEUE="$QUEUE_ROOT/$RUNID"
mkdir -p "$QUEUE"

if [[ ! -d "$INCOMING" ]]; then
    install -d -o bloomer -g bloomer -m 0755 "$INCOMING"
fi

############################
# CLAIM READY FILES (scan -> log -> move)
############################

READY_LIST="$(mktemp -p /tmp ingest-ready.XXXXXX)"
CLAIM_COUNT=0

while IFS= read -r -d '' file; do
    if is_stable "$file"; then
        printf '%s\0' "$file" >> "$READY_LIST"
        CLAIM_COUNT=$((CLAIM_COUNT + 1))
    fi
done < <(find "$INCOMING" -type f -print0)

if (( CLAIM_COUNT == 0 )); then
    rm -f "$READY_LIST"
    rmdir "$QUEUE" 2>/dev/null || true
    exit 0
fi

log "==== RUN START ($CLAIM_COUNT files claimed) ===="

while IFS= read -r -d '' file; do
    rel="${file#$INCOMING/}"
    dest="$QUEUE/$rel"
    mkdir -p "$(dirname "$dest")"
    mv "$file" "$dest"
done < "$READY_LIST"

rm -f "$READY_LIST"

# Clean up empty directories left behind in incoming (after claiming files)
find "$INCOMING" -depth -type d -empty -delete 2>/dev/null || true

############################
# BEETS PASS
############################

if ! find "$QUEUE" -type f \( -iname '*.mp3' -o -iname '*.flac' -o -iname '*.wav' -o -iname '*.m4a' -o -iname '*.ogg' -o -iname '*.opus' -o -iname '*.aac' -o -iname '*.aif' -o -iname '*.aiff' -o -iname '*.wma' \) -print -quit | grep -q .; then
    log "Skipping beets (no obvious audio files in queue)"
else
    log "Running beets..."
    beet import -q "$QUEUE" >> "$LOGFILE" 2>&1 || true
fi

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
            mime=$(file -b --mime-type "$file" 2>/dev/null || true)
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

        # determine loose vs tree (directly under incoming = loose; deeper = tree)
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
# UNMATCHED AUDIO PASS (after video, before misc)
############################

AUDIO_BUCKET="$MUSIC_UNMATCHED_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d '' file; do
    rel="${file#$QUEUE/}"
    ext="${file##*.}"
    ext_lower=$(echo "$ext" | tr '[:upper:]' '[:lower:]')

    is_audio=0

    case "$ext_lower" in
        # mp4/m4v are owned by the VIDEO pass decision (ffprobe). Never treat as audio here.
        mp4|m4v)
            is_audio=0
            ;;
        wav|flac|mp3|m4a|aac|ogg|opus|alac|aiff|aif|wma)
            is_audio=1
            ;;
        *)
            mime=$(file -b --mime-type "$file" 2>/dev/null || true)
            if [[ "$mime" == audio/* ]]; then
                is_audio=1
            fi
            ;;
    esac

    if (( is_audio == 1 )); then
        # tree vs loose (same rule as video)
        if [[ "$rel" == */* ]]; then
            dest="$MUSIC_UNMATCHED_ROOT/$rel"
        else
            dest="$AUDIO_BUCKET/$rel"
        fi

        if copy_then_remove "$file" "$dest"; then
            log "AUDIO_UNMATCHED -> $dest"
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
