#!/bin/bash -ex

function retry {
    local attempt=1
    local command="${@}"

    while [ $attempt -ne 10 ]; do
        echo "(run $attempt/10) running $command"
        $command
        if [[ $? -eq 0 ]]; then
            echo "(success) $command"
            break
        else
            ((attempt++))
            echo "(FAILED) $command"
            sleep 3
        fi
    done
}

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
else
    retry pip install coverage python-coveralls mock --quiet
    if [[ $PYFARM_AGENT_TEST_HTTP_SCHEME == "https" ]]; then
        retry pip install PyOpenSSL service_identity --quiet
    fi
    retry pip install -e . --egg --quiet
    pip freeze
fi
