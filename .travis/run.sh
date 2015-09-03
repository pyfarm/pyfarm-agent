#!/bin/bash -e

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate pyfarm-agent
fi

coverage run --branch `which trial` --reporter=bwverbose tests/test_agent
mv -v .coverage .coverage.1
coverage run --branch `which trial` --reporter=bwverbose tests/test_jobtypes
mv -v .coverage .coverage.2
coverage combine

