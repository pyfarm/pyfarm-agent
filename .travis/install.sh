#!/bin/bash -e

if [[ "$(uname -s)" == 'Darwin' ]]; then
    brew update || brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv

    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi

    case "${TRAVIS_PYTHON_VERSION}" in
        2.6)
            pyenv install 2.6.9
            pyenv virtualenv 2.6.9 pyfarm-agent
        ;;
        2.7)
            pyenv install 2.7.10
            pyenv virtualenv 2.7.10 pyfarm-agent
        ;;
    esac
    pyenv rehash
    pyenv activate pyfarm-agent
fi

pip install coverage python-coveralls mock --quiet
pip install -e . --egg --quiet
pip freeze
