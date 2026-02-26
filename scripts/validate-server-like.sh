#!/usr/bin/env bash
set -euo pipefail

composer install --prefer-dist --no-interaction --ignore-platform-reqs
php ./tempest data:validate
