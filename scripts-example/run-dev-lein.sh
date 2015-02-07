export INPUT_LOCATION=`pwd`/example/hundred/input
export OUTPUT_LOCATION=`pwd`/example/hundred/output
export REDACT=true
export DEV_LOCAL=TRUE
export REDACT=false
export DEV_LOCAL=TRUE
export MONTH=true
export DAY=true
# export YEAR=true
export ALLTIME=true


rm -rf $OUTPUT_LOCATION

lein run