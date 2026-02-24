#!/usr/bin/env bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
set -euo pipefail

############################
# CONFIG
############################

INCOMING="/ssdtemp/incoming"
QUEUE_ROOT="/ssdtemp/.ingest-queue"

VIDEOS_ROOT="/herd/family/videos"
MUSIC_ROOT="/herd/family/music"
MUSIC_UNMATCHED_ROOT="/herd/family/music/unmatched"

IMAGES_ROOT="/herd/family/images"
MISC_ROOT="/herd/family/misc"

LOGFILE="/var/log/media-ingest.log"

# Claim rule for rsync -avP:
# Use CTIME, not MTIME (rsync -a preserves old mtimes, so -mmin is useless here).
CLAIM_CMIN=1

BIGFILE_BYTES=$((3 * 1024 * 1024 * 1024)) # 3GB
LOCKFILE="/run/lock/media-ingest.lock"

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
    mp3|flac|wav|m4a|ogg|opus|aac|aif|aiff|wma|alac) ;;
    *) return 1 ;;
  esac

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

# For images/misc: if dest exists, append -dupe-N before extension
unique_path() {
  local dest="$1"
  if [[ ! -e "$dest" ]]; then
    printf '%s' "$dest"
    return 0
  fi

  local dir base name ext candidate n
  dir="$(dirname "$dest")"
  base="$(basename "$dest")"

  if [[ "$base" == *.* ]]; then
    name="${base%.*}"
    ext=".${base##*.}"
  else
    name="$base"
    ext=""
  fi

  n=1
  while :; do
    candidate="$dir/${name}-dupe-$n$ext"
    [[ ! -e "$candidate" ]] && { printf '%s' "$candidate"; return 0; }
    n=$((n+1))
  done
}

copy_then_remove_unique() {
  local src="$1"
  local dest="$2"

  mkdir -p "$(dirname "$dest")"
  dest="$(unique_path "$dest")"

  if rsync -a "$src" "$dest"; then
    rm -f "$src"
    return 0
  fi
  return 1
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

# Ensure incoming exists
if [[ ! -d "$INCOMING" ]]; then
  install -d -m 0755 "$INCOMING"
fi

# Resume oldest existing queue first (handles interrupted runs cleanly)
existing_queue="$(find "$QUEUE_ROOT" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort | head -n1 || true)"
if [[ -n "${existing_queue:-}" ]]; then
  QUEUE="$existing_queue"
  RUNID="$(basename "$QUEUE")"
else
  RUNID="$(date '+%Y-%m-%d-%H%M%S')"
  QUEUE="$QUEUE_ROOT/$RUNID"
  mkdir -p "$QUEUE"

  ############################
  # CLAIM READY FILES (ctime-gated)
  ############################

  READY_LIST="$(mktemp -p /tmp ingest-ready.XXXXXX)"
  find "$INCOMING" -type f -cmin +"$CLAIM_CMIN" -print0 > "$READY_LIST"

  CLAIM_COUNT="$(count_nul_entries "$READY_LIST")"
  if (( CLAIM_COUNT == 0 )); then
    rm -f "$READY_LIST"
    rmdir "$QUEUE" 2>/dev/null || true
    exit 0
  fi

  log "==== RUN START ($CLAIM_COUNT files claimed) ===="

  while IFS= read -r -d $'\0' file; do
    rel="${file#$INCOMING/}"
    dest="$QUEUE/$rel"
    mkdir -p "$(dirname "$dest")"
    mv "$file" "$dest"
  done < "$READY_LIST"

  rm -f "$READY_LIST"
  find "$INCOMING" -depth -type d -empty -delete 2>/dev/null || true
fi

TIMESTAMP_DIR="$(date '+%Y-%m-%d-%H')"

############################
# BEETS PASS (only sane audio)
############################

SANE_AUDIO_LIST="$(mktemp -p /tmp ingest-audio.XXXXXX)"
: > "$SANE_AUDIO_LIST"

while IFS= read -r -d $'\0' f; do
  if is_sane_audio_file "$f"; then
    printf '%s\0' "$f" >> "$SANE_AUDIO_LIST"
  fi
done < <(find "$QUEUE" -type f -print0)

AUDIO_COUNT="$(count_nul_entries "$SANE_AUDIO_LIST")"
if (( AUDIO_COUNT > 0 )); then
  log "Running beets on $AUDIO_COUNT sane audio files..."
  while IFS= read -r -d $'\0' af; do
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

VIDEO_BUCKET="$VIDEOS_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d $'\0' file; do
  rel="${file#$QUEUE/}"
  ext="${file##*.}"
  ext_lower="$(echo "$ext" | tr '[:upper:]' '[:lower:]')"
  size="$(stat -c %s "$file" 2>/dev/null || echo 0)"

  is_video=0
  case "$ext_lower" in
    mkv|avi|mov|webm|ts|m2ts|mpg|mpeg|wmv) is_video=1 ;;
    mp4|m4v) has_video_stream "$file" && is_video=1 ;;
    *)
      mime="$(file -b --mime-type "$file" 2>/dev/null || true)"
      [[ "$mime" == video/* ]] && is_video=1
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
# LEFTOVER AUDIO PASS
#
# What you asked for:
# - if it's audio AND it is in a directory tree -> copy into MUSIC_ROOT preserving tree
# - if it's audio but "loose" at queue root -> MUSIC_UNMATCHED_ROOT/timestamp
############################

AUDIO_BUCKET="$MUSIC_UNMATCHED_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d $'\0' file; do
  rel="${file#$QUEUE/}"

  if is_sane_audio_file "$file"; then
    if [[ "$rel" == */* ]]; then
      dest="$MUSIC_ROOT/$rel"
    else
      dest="$AUDIO_BUCKET/$rel"
    fi

    if copy_then_remove "$file" "$dest"; then
      log "AUDIO_LEFTOVER -> $dest"
    fi
  fi
done < <(find "$QUEUE" -type f -print0)

############################
# IMAGES PASS (dupe rename)
############################

IMAGES_BUCKET="$IMAGES_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d $'\0' file; do
  rel="${file#$QUEUE/}"
  ext="${file##*.}"
  ext_lower="$(echo "$ext" | tr '[:upper:]' '[:lower:]')"

  is_img=0
  case "$ext_lower" in
    jpg|jpeg|png|gif|webp|bmp|tif|tiff|heic) is_img=1 ;;
    *)
      mime="$(file -b --mime-type "$file" 2>/dev/null || true)"
      [[ "$mime" == image/* ]] && is_img=1
      ;;
  esac

  if (( is_img == 1 )); then
    # keep tree if it had one, otherwise timestamp bucket
    if [[ "$rel" == */* ]]; then
      dest="$IMAGES_ROOT/$rel"
    else
      dest="$IMAGES_BUCKET/$rel"
    fi

    if copy_then_remove_unique "$file" "$dest"; then
      log "IMAGE -> $dest"
    fi
  fi
done < <(find "$QUEUE" -type f -print0)

############################
# MISC PASS (only leftovers, dupe rename)
############################

MISC_BUCKET="$MISC_ROOT/$TIMESTAMP_DIR"

while IFS= read -r -d $'\0' file; do
  rel="${file#$QUEUE/}"
  dest="$MISC_BUCKET/$rel"

  if copy_then_remove_unique "$file" "$dest"; then
    log "MISC -> $dest"
  fi
done < <(find "$QUEUE" -type f -print0)

############################
# CLEANUP
############################

find "$QUEUE" -type d -empty -delete 2>/dev/null || true
rmdir "$QUEUE" 2>/dev/null || true

log "==== RUN END ($RUNID) ===="
