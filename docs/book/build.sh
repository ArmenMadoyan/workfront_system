#!/usr/bin/env bash
# Assemble all chapters into one markdown file, then PDF if pandoc is available.
#   bash docs/book/build.sh
set -euo pipefail
cd "$(dirname "$0")"

OUT="workfront-system-design.md"
CHAPTERS=(
  00-index.md
  part1/01-problem-and-requirements.md
  part1/02-data-model-and-schema.md
  part1/03-api-layer.md
  part1/04-eventing-core.md
  part1/05-concurrency-and-integrity.md
  part1/06-realtime-fanout.md
  part1/07-observability.md
  part1/08-performance-and-scale.md
  part2/09-microservices-and-communication.md
  part2/10-messaging-patterns.md
  part2/11-design-patterns.md
  part2/12-caching.md
  part2/13-datastores.md
  part2/14-webhooks-at-scale.md
  part2/15-infra-and-cloud.md
  part2/16-cicd-and-ops.md
  part3/17-mock-the-main-question.md
  part3/18-rapid-fire-qa.md
  appendix/19-glossary-and-decision-log.md
)

: > "$OUT"
for ch in "${CHAPTERS[@]}"; do
  if [ -f "$ch" ]; then
    cat "$ch" >> "$OUT"
    printf '\n\n---\n\n' >> "$OUT"
  else
    echo "WARN: missing $ch" >&2
  fi
done
echo "combined -> docs/book/$OUT ($(wc -l < "$OUT") lines)"

if command -v pandoc >/dev/null 2>&1; then
  pandoc "$OUT" -o workfront-system-design.pdf \
    --toc --toc-depth=2 -V geometry:margin=2cm 2>/dev/null \
    && echo "pdf     -> docs/book/workfront-system-design.pdf" \
    || echo "pandoc present but PDF engine missing (try: brew install basictex)"
else
  echo "pandoc not installed -> skipping PDF. Install with: brew install pandoc"
  echo "(the combined .md is printable from VS Code / browser as-is)"
fi
