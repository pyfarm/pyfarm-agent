#!/bin/bash -e

if which pyenv > /dev/null; then
    eval "$(pyenv init -)"
fi
pyenv activate pyfarm-agent
