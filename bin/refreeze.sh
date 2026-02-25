#!/usr/bin/env bash

# Rebuild the container, installing the Python packages listed in requirements.top and updating
# their dependencies, then "freeze" those versions into requirements.txt.

set -ev

cp requirements.{top,txt}
docker-compose build ufc-elo-calculator
docker-compose run --rm --no-deps ufc-elo-calculator sh -c 'pip freeze > requirements.txt'
sed -nE 's/(^[^#].*egg=(.*))/s%\2=.*%\1%/p' requirements.top >/tmp/github-urls
sed -f /tmp/github-urls requirements.txt >requirements.new
mv requirements.{new,txt}
rm /tmp/github-urls

# (the `sed` commands replace the github package names with their URLs from requirements.top)
