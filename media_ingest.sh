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

# File stability rules:
# - mtime must be at least STABILITY_SECONDS old
# - additionally, file must not change (size+mtime) across one global sample window
STABILITY_SECONDS=44
STABILITY_SAMPLE_SECONDS=3

BIGFILE_BYTES=$((3 * 1024 * 1024 * 1024)) # 3GB
LOCKFILE="/run/lock/ingest-media.lock"

############################
# HELPERS
############################

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOGFILE"
}

has_video_stream() {
    ffprobe -v error -select_streams v \
        -show_entries stream=codec_type \
        -of csv=p=0 "$1" 2>/dev/null | grep -q video
}

probe_sanity() {
    timeout 10 ffprobe -v error "$1" >/dev/null 2>&1
}

# Only treat as "sane audio" if:
# - extension suggests audio AND
# - ffprobe can parse AND
# - at least one audio stream exists
is_sane_audio_file() {
    local f="$1"
    local ext="${f##*.}"
    local ext_lower
    ext_lower="$(echo "$ext" | tr '[:upper:]' '[:lower:]')"

    case "$ext_lower" in
        mp3|flac|wav|m4a|ogg|opus|aac|aif|aiff|wma|alac)
            ;;
        *)
            return 1
            ;;
    esac

    # Don't let a single broken file hang the run.
    # "ffprobe -show_streams" is heavier; keep it minimal but decisive.
    timeout 10 ffprobe -v error -select_streams a \
        -show_entries stream=codec_type \
        -of csv=p=0 "$f" 2>/dev/null | grep -q '^audio$'
}

# Copy to destination (creating dirs), then delete source on success.
# If destination exists and sizes match, treat as duplicate and delete source.
# If destination exists but does not match, do nothing and return failure.
copy_then_remove() {
    local src="$1"
    local dest="$2"

    mkdir -p "$(dirname "$dest")"

    if [[ -e "$dest" ]]; then
        local s_src s_dst
        s_src=$(stat -c %s "$src" 2>/dev/null || echo 0)
        s_dst=$(stat -c %s "$dest" 2>/dev/null || echo -1)
        if [[ "$s_src" -eq "$s_dst" && "$s_src" -gt 0 ]]; then
            rm -f "$src"
            return 0
        fi
        return 1
    fi

    if rsync -a "$src" "$dest"; then
        rm -f "$src"
        return 0
    fi

    return 1
}

# Build a NUL-separated list of "stable" files under INCOMING.
# This avoids sleeping per-file; it sleeps once globally.
build_ready_list() {
    local incoming="$1"
    local out_list="$2"

    : > "$out_list"

    local snap1
    snap1="$(mktemp -p /tmp ingest-snap1.XXXXXX)"

    # Record triples: path\0size\0mtime_epoch\0
    # xargs -0 -r prevents running stat if find yields nothing.
    find "$incoming" -type f -print0 \
      | xargs -0 -r stat -c '%n\0%s\0%Y\0' \
      > "$snap1"

    [[ -s "$snap1" ]] || { rm -f "$snap1"; return 0; }

    sleep "$STABILITY_SAMPLE_SECONDS"

    local now
    now="$(date +%s)"

    while IFS= read -r -d '' path \
       && IFS= read -r -d '' s1 \
       && IFS= read -r -d '' m1
    do
        [[ -e "$path" ]] || continue

        # mtime must be old enough
        if (( now - m1 < STABILITY_SECONDS )); then
            continue
        fi

        # Re-stat once
        local s2 m2
        s2="$(stat -c %s "$path" 2>/dev/null || echo '')"
        m2="$(stat -c %Y "$path" 2>/dev/null || echo '')"
        [[ -n "$s2" && -n "$m2" ]] || continue

        # unchanged across window
        if [[ "$s1" == "$s2" && "$m1" == "$m2" ]]; then
            printf '%s\0' "$path" >> "$out_list"
        fi
    done < "$snap1"

    rm -f "$snap1"
}

count_nul_entries() {
    local f="$1"
    tr -cd '\0' < "$f" | wc -c
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
# CLAIM READY FILES (snapshot -> move to queue)
############################

READY_LIST="$(mktemp -p /tmp ingest-ready.XXXXXX)"
build_ready_list "$INCOMING" "$READY_LIST"
CLAIM_COUNT="$(count_nul_entries "$READY_LIST")"

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
# BEETS PASS (only sane audio)
############################

SANE_AUDIO_LIST="$(mktemp -p /tmp ingest-audio.XXXXXX)"
: > "$SANE_AUDIO_LIST"

# Build a list of *sane* audio files we want beets to touch.
while IFS= read -r -d '' f; do
    if is_sane_audio_file "$f"; then
        printf '%s\0' "$f" >> "$SANE_AUDIO_LIST"
    fi
done < <(find "$QUEUE" -type f -print0)

AUDIO_COUNT="$(count_nul_entries "$SANE_AUDIO_LIST")"

if (( AUDIO_COUNT == 0 )); then
    log "Skipping beets (no sane audio files in queue)"
else
    log "Running beets on $AUDIO_COUNT sane audio files..."

    # Run beets per-file so it never gets a chance to walk video/other stuff.
    # -q quiet
    # Any single-file failure should not kill the whole run.
    while IFS= read -r -d '' af; do
        beet import -q "$af" >> "$LOGFILE" 2>&1 || {
            log "Beets failed (continuing): ${af#$QUEUE/}"
            true
        }
    done < "$SANE_AUDIO_LIST"
fi

rm -f "$SANE_AUDIO_LIST"

############################
# VIDEO PASS
############################

TIMESTAMP_DIR=$(date '+%Y-%m-%d-%H')
VIDEO_BUCKET="$VIDEOS_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d '' file; do
    rel="${file#$QUEUE/}"
    ext="${file##*.}"
    ext_lower=$(echo "$ext" | tr '[:upper:]' '[:lower:]')
    size=$(stat -c %s "$file" 2>/dev/null || echo 0)

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
        if (( size > BIGFILE_BYTES )); then
            if ! probe_sanity "$file"; then
                log "Sanity probe failed: $rel"
                continue
            fi
        fi

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
            # only if it's actually sane audio (not corrupt / misdetected)
            if is_sane_audio_file "$file"; then
                is_audio=1
            fi
            ;;
        *)
            mime=$(file -b --mime-type "$file" 2>/dev/null || true)
            if [[ "$mime" == audio/* ]]; then
                # still require ffprobe audio stream sanity
                if timeout 10 ffprobe -v error -select_streams a \
                    -show_entries stream=codec_type -of csv=p=0 "$file" 2>/dev/null | grep -q '^audio$'
                then
                    is_audio=1
                fi
            fi
            ;;
    esac

    if (( is_audio == 1 )); then
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

find "$QUEUE" -type d -empty -delete 2>/dev/null || true
rmdir "$QUEUE" 2>/dev/null || true

log "==== RUN END ===="
