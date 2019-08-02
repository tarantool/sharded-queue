#!/bin/sh

set -e

tarantoolctl rocks install ldecnumber 1.1.3-1
tarantoolctl rocks install metrics 0.1.3-1
tarantoolctl rocks install cluster 0.9.2-1
tarantoolctl rocks install space-explorer 1.0.2-1
