#!/bin/bash -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate virtualenv
fi

coverage run --branch `which trial` tests/
