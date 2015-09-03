#!/bin/bash -e

if [[ "$(uname -s)" == 'Darwin' ]]; then
    brew update || brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv

    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi

    pyenv install $PYTHON_VERSION
    pyenv virtualenv $PYTHON_VERSION virtualenv
    pyenv rehash
    pyenv activate virtualenv
fi

pip install coverage python-coveralls mock
pip install .
pip freeze
