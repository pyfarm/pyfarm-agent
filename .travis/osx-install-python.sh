#!/bin/bash -e

brew update || brew update
brew outdated pyenv || brew upgrade pyenv
brew install pyenv-virtualenv

if which pyenv > /dev/null; then eval "$(pyenv init -)" fi

case "${TRAVIS_PYTHON_VERSION}" in
    2.6)
        pyenv install $PYTHON_VERSION
        pyenv virtualenv 2.6.9 pyfarm-agent
    ;;
    2.7)
        pyenv install 2.7.10
        pyenv virtualenv 2.7.10 pyfarm-agent
    ;;
esac
