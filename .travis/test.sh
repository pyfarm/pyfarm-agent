#!/bin/bash -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate virtualenv
fi

if [[ "$READTHEDOCS" == "1" ]]; then
    make -C docs html
else
    coverage run --branch `which trial` tests/
fi
