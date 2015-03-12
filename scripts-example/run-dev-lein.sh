export INPUT_LOCATION=`pwd`/example/hundred/input/hundred.txt
export OUTPUT_LOCATION=`pwd`/example/hundred/output
export REDACT=true
export DEV_LOCAL=TRUE
export REDACT=false
export DEV_LOCAL=TRUE
export LASKURI_TASKS="[[:ever-doi-first-date :all-time] [:doi-domain-periods-count :month] [:top-domains :month] [:doi-periods-count :day] [:domain-periods-count :day] [:subdomain-periods-count :day] [:top-domains :day]]"


rm -rf $OUTPUT_LOCATION

lein run

ls $OUTPUT_LOCATION