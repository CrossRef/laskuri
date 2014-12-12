export TIMESTAMP=`date +%s`
export INPUT_LOCATION=s3n://resolutionlogs-demo-oregon/access_log_201210_cr5-hundred
export OUTPUT_LOCATION=s3n://laskuri-output-demo-oregon/$TIMESTAMP
export REDACT=true
export AWS_ACCESS_KEY_ID=AKIAJXIU5EJM36C7K36Q
export AWS_SECRET_ACCESS_KEY=XACs5zRpZDehZ4M9nTeSjcfq8f8b5Fl5ldIaDYlK

lein run