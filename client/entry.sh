#!/bin/sh
# Install the one dependency, then run whatever command was given.
set -e
pip install -q --disable-pip-version-check --root-user-action=ignore -r /work/client/requirements.txt
exec "$@"
