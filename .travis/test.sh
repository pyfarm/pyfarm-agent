#!/bin/bash -e

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate virtualenv
fi

pwd
env PYTHONPATH=. coverage run --branch `which trial` --reporter=bwverbose tests/test_agent
mv -v .coverage .coverage.1
env PYTHONPATH=. coverage run --branch `which trial` --reporter=bwverbose tests/test_jobtypes
mv -v .coverage .coverage.2
coverage combine
