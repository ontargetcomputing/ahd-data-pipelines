#!/bin/bash

# Run apt-get update and install libkrb5-dev package
sudo apt-get update
sudo apt-get install -y libkrb5-dev

# Upgrade pip and install/upgrade Cython
pip install --upgrade pip
pip install --upgrade cython