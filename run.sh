#!/bin/bash

rm -rf dev/*
./stop.sh
./start.sh && ./assemble.sh
