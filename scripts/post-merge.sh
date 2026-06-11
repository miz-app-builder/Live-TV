#!/bin/bash
set -e

npm install --legacy-peer-deps
cp server.js live-tv/server.js
