#!/bin/bash

set -e

if [ ! -z $VIRTUAL_ENV ]; then
	echo "Please deactivate your virtualenv before setting this one up"
	exit 1
fi

VENV_DIR=venv

virtualenv $VENV_DIR
. $VENV_DIR/bin/activate

pip install -r requirements.txt

# reinstall ty.social_radar to make sure that the latest code is running
if [ `pip freeze | grep tymongoimport | wc -l` -eq 1 ]; then
	pip uninstall --yes tymongoimport
fi
