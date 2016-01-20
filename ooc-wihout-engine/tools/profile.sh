#!/bin/bash
#
# Execute the application.
# Run this from the ooc-wihout-engine directory.
#
# Connor Imes
# 2016-01-19
#

BINARY="./bin/pagerank"
ARGUMENTS="../soc-LiveJournal1.txt"

TRIALS=4
SLEEP_TIME=10

if [ ! -e $BINARY ]; then
  echo "$BINARY not found - please run from build directory (after building)"
  exit 1
fi

PROFILER_SCRIPT=$1
if [ -z $PROFILER_SCRIPT ]; then
  echo "Usage:"
  echo "  $0 <he-profiler script>"
  exit 1
fi

command="${BINARY} ${ARGUMENTS}"
profile_cmd="python $PROFILER_SCRIPT -c \"$command\" -g $SLEEP_TIME -t $TRIALS"
echo $profile_cmd
eval $profile_cmd
if [ $? -ne 0 ]; then
  echo "Bad exit code - aborting"
  exit 1
fi
