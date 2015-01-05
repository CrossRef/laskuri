export TIMESTAMP=`date +%s`
export INPUT_LOCATION=s3n://logs-bucket/location/log-files
export OUTPUT_LOCATION=s3n://logs-output-bucket/location/$TIMESTAMP
export REDACT=true
export AWS_ACCESS_KEY_ID=TODO
export AWS_SECRET_ACCESS_KEY=TODO
export DEV_LOCAL=TRUE

lein run