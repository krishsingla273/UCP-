#!/usr/bin/env bash
# The built-in demo is now `./rtp demo` (no shell needed). This wrapper just builds and runs it.
cd "$(dirname "$0")" && make -s && ./rtp demo "$@"
