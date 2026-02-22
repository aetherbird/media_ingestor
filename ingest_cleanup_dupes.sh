#!/usr/bin/env bash
set -euo pipefail

QUEUE_ROOT="/ssdtemp/.ingest-queue"
VIDEOS_ROOT="/herd/family/videos"

# For any queued file, if the same relative path already exists in videos
# and the sizes match, delete the queued copy.
find "$QUEUE_ROOT" -type f -print0 | while IFS= read -r -d '' src; do
  rel="${src#${QUEUE_ROOT}/}"
  rel="${rel#*/}" # strip the RUNID directory
  dst="$VIDEOS_ROOT/$rel"

  [[ -e "$dst" ]] || continue

  s_src=$(stat -c %s "$src" 2>/dev/null || echo 0)
  s_dst=$(stat -c %s "$dst" 2>/dev/null || echo -1)

  if [[ "$s_src" -gt 0 && "$s_src" -eq "$s_dst" ]]; then
    rm -f -- "$src"
  fi
done

# Remove empty directories after deletions
find "$QUEUE_ROOT" -depth -type d -empty -delete 2>/dev/null || true
