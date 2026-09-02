#!/usr/bin/env bash

set -euo pipefail

install_make() {
  local privilege=()

  if [[ ${EUID} -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
      echo "Error: installing make requires root access or sudo." >&2
      exit 1
    fi
    privilege=(sudo)
  fi

  echo "make is not installed; installing it now..."
  if command -v apt-get >/dev/null 2>&1; then
    "${privilege[@]}" apt-get update
    "${privilege[@]}" apt-get install -y make
  elif command -v dnf >/dev/null 2>&1; then
    "${privilege[@]}" dnf install -y make
  elif command -v yum >/dev/null 2>&1; then
    "${privilege[@]}" yum install -y make
  elif command -v pacman >/dev/null 2>&1; then
    "${privilege[@]}" pacman -Sy --noconfirm make
  elif command -v brew >/dev/null 2>&1; then
    brew install make
  else
    echo "Error: no supported package manager was found to install make." >&2
    exit 1
  fi
}

if ! command -v make >/dev/null 2>&1; then
  install_make
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: uv is required but is not installed." >&2
  echo "Install it from https://docs.astral.sh/uv/getting-started/installation/ and try again." >&2
  exit 1
fi

echo "Installing application and development requirements from uv.lock..."
uv sync --frozen --all-groups
echo "Requirements installed successfully."
