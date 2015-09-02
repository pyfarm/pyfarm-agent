#!/bin/bash

set -e
set -x

if [[ "$(uname -s)" == 'Darwin' ]]; then
    brew update || brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv

    if which pyenv > /dev/null; then
        eval "$(pyenv init -)"
    fi

    case "${PYVER}" in
        py26)
            pyenv install 2.6.9
            pyenv virtualenv 2.6.9 pyfarm-agent
            ;;
        py27)
            pyenv install 2.7.10
            pyenv virtualenv 2.7.10 pyfarm-agent
            ;;
    esac
    pyenv rehash
    pyenv activate pyfarm-agent
fi

#curl -o retry.sh https://raw.githubusercontent.com/pyfarm/pyfarm-build/master/travis/retry.sh

#if [[ $TRAVIS_PYTHON_VERSION == '2.6' ]] || [[ $PYVER == 'py26' ]]; then
#    pip install -U ipaddress unittest2 mock==1.0.1
#elif [[ $TRAVIS_PYTHON_VERSION == '2.7' ]] || [[ $PYVER == 'py27' ]]; then
#    pip install -U ipaddress mock
#elif [[ $TRAVIS_PYTHON_VERSION == '3.2' ]] || [[ $PYVER == 'py32' ]]; then
#    pip install -U ipaddress mock
#elif [[ $TRAVIS_PYTHON_VERSION == '3.3' ]] || [[ $PYVER == 'py33' ]]; then
#    pip install -U ipaddress
#fi

pip install coverage coveralls