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

pip install --upgrade pip
pip install wheel coverage==3.7.1 python-coveralls mock
python setup.py develop
pip freeze
