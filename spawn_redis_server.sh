#!/bin/sh
#
# DON'T EDIT THIS!
#
# CodeCrafters uses this file to test your code. Don't make any changes here!
#
# DON'T EDIT THIS!
RUST_BACKTRACE=full exec cargo run \
    --quiet \
    --target-dir=/tmp/codecrafters-redis-target \
    --manifest-path $(dirname $0)/Cargo.toml \
    -- "$@"
