export TIMESTAMP=`date +%s`
export INPUT_LOCATION=s3n://logs-bucket/location/log-files
export OUTPUT_LOCATION=s3n://logs-output-bucket/location/$TIMESTAMP
export REDACT=true
export AWS_ACCESS_KEY_ID=TODO
export AWS_SECRET_ACCESS_KEY=TODO
export LASKURI_TASKS="[[:ever-doi-first-date :all-time] [:doi-domain-periods-count :month] [:top-domains :month] [:doi-periods-count :day] [:domain-periods-count :day] [:subdomain-periods-count :day] [:top-domains :day]]"

spark-submit --class laskuri.core --name "Laskuri" --master local ./target/uberjar/laskuri-0.1.0-SNAPSHOT-standalone.jar