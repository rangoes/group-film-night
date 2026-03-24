#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: ./publish-to-github.sh <repo-url>"
  exit 1
fi

REPO_URL="$1"

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REPO_URL"
else
  git remote add origin "$REPO_URL"
fi

git push -u origin main
