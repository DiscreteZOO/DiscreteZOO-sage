#!/usr/bin/env bash

FILE="master.zip"
LINK="https://github.com/DiscreteZOO/DiscreteZOO-spec/archive/${FILE}"
SPECPATH="discretezoo/spec/"

if [ -n "$(which git)" ] && git status -s > /dev/null 2> /dev/null; then
    git submodule init &&
    git submodule update
else
    if [ -z "$(which wget)" -o -z "$(which unzip)" ]; then
        echo "Error: the required commands are not found." >&2
        echo "To make DiscreteZOO work, please download ${LINK} and extract the *.json files into ${SPECPATH}" >&2
        exit 1
    fi
    wget "${LINK}" &&
    unzip -j "${FILE}" -d "${SPECPATH}" &&
    rm "${FILE}"
fi &&
echo "Specification files successfully loaded. DiscreteZOO is now ready to use!"
