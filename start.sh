#!/usr/bin/env bash

set -e

docker compose down -v

docker rmi -f smart_chat_agent-app:latest

docker compose up -d

echo "Application Restarted ~"