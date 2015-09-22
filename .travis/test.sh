#!/bin/bash -e

if [[ "$(uname -s)" == 'Darwin' ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate virtualenv
fi

coverage run --branch `which trial` tests/
