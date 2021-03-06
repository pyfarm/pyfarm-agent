#!/bin/bash -e

if [[ "$READTHEDOCS" == "1" ]]; then
    make -C docs html
fi

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi
    pyenv activate virtualenv
    coverage run --branch $VIRTUAL_ENV/bin/trial tests

else
    coverage run --branch `which trial` tests/
fi
