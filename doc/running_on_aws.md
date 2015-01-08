# Running on AWS

Spark [plays nicely with Amazon AWS](https://spark.apache.org/docs/latest/ec2-scripts.html). Here are the steps to run it at CrossRef. We are storing the input log files in S3 at `s3://resolution-logs` and the results at `s3://resolution-logs-output`. These are in the Oregon AWS Region.

There may be better ways to copy multiple files around, but these are known to work.

## Environment & First time setup

- login to the AWS console. Make sure you've selected 'Oregon' region.
- get an AWS key pair from the 'Identity and Access Management' console
- set the AWS key in your environment (do this each run)

      export AWS_ACCESS_KEY_ID=KEY_HERE
      export AWS_SECRET_ACCESS_KEY=KEY_HERE
      
- get an EC2 key pair (different but similarly named) from the 'EC2' console. If you have trouble using it, check you created them in Oregon.
- download the key pair and put the private key in `~/.ssh/laskuri-oregon` or something similar, `chmod 600`.
- [download Spark](https://spark.apache.org/releases/spark-release-1-1-1.html) and place it, for example in `~/Downloads`

## Compile and upload to S3

It's easier to copy the code in via S3. You only need to do this when you've changed the code.

      lein uberjar
      aws s3 cp ./target/uberjar/laskuri-0.1.0-SNAPSHOT-standalone.jar s3://crossref-code

## Start cluster

- Start a cluster with 10 slaves in `us-west-2`, which is in Oregon. Important. Replace key path if you need to (and in subsequent commands).

       ~/Downloads/spark-1.1.1-bin-hadoop2.4/ec2/spark-ec2 -k laskuri-oregon -i ~/.ssh/laskuri-oregon.pem --instance-type=m1.large  -s 10 -r us-west-2 --ebs-vol-size=400 launch laskuri


This will take a few minutes longer than you think.

- Log into the master node

      ~/Downloads/spark-1.1.1-bin-hadoop2.4/ec2/spark-ec2 -k laskuri-oregon -i ~/.ssh/laskuri-oregon.pem -r us-west-2 login laskuri

## Configure

 - Configuration is via environment variables, and these need to be available to every node, including master and slaves. Edit the environment file on the master:

       vi  /root/spark/conf/spark-env.sh
       
   and add
   
       export INPUT_LOCATION=/input/
       export OUTPUT_LOCATION=/output/
       export REDACT=false
       export ALLTIME=true
       export DAY=true
       export YEAR=false
       export MONTH=true
       export AWS_ACCESS_KEY_ID=KEY_HERE
       export AWS_SECRET_ACCESS_KEY=KEY_HERE
      
   plus any extra Laskuri config stuff. The `INPUT_LOCATION` and `OUTPUT_LOCATION` are locations within HDFS not the actual file system.
   
 - This file will be copied to the slaves.
 - Set the AWS keys locally on the master:
 
       export AWS_ACCESS_KEY_ID=KEY_HERE
       export AWS_SECRET_ACCESS_KEY=KEY_HERE
 
 - Next, the AWS tools are out of date on the stock Amazon image (surprise!). Upgrade them.

       pip install awscli --upgrade
       yum install tmux
       yum install links
       
# Pull in code, data, distribute
- Download the JAR from S3 to the master

      mkdir /root/spark/code
      aws s3 cp s3://crossref-code/laskuri-0.1.0-SNAPSHOT-standalone.jar /root/spark/code

- Deploy the JAR and the configuration file to the slaves

      ~/spark-ec2/copy-dir /root/spark/conf
      ~/spark-ec2/copy-dir /root/spark/code
      
- Get the data in from S3. Check you have enough space on the local filesystem:
 
      df -h

- Copy the files to the local filesystem. 

  Single file:
  
      aws s3 cp  s3://resolution-logs/a-single-file /mnt/input

  All files:
  
      aws s3 sync s3://resolution-logs/ /mnt/input
      
  Copying all files (about 200 GB) takes about an hour and a half. 
- Have a number of cups of tea.
- Copy the files from the real file system to the distributed HDFS file system

      ephemeral-hdfs/bin/hadoop fs -copyFromLocal /mnt/input /input

- Probably enter `tmux`
- Run the job!

      time spark/bin/spark-submit --class laskuri.core --name "Laskuri" --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.executor.extraJavaOptions "-XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled" /root/spark/code/laskuri-0.1.0-SNAPSHOT-standalone.jar
      
- Monitor

      links http://localhost:8080

- When cooked, check the output in HDFS

      ephemeral-hdfs/bin/hadoop fs -ls /

- Copy the output from HDFS into the local file sytsem.

      mkdir /output
      ephemeral-hdfs/bin/hadoop fs -copyToLocal /output/ /
      
- Delete from HDFS if you want

      ephemeral-hdfs/bin/hadoop fs -rmr /output/
      
- And then from local filesytem to S3

      aws s3 sync /output/ s3://resolution-logs-output/`date +%Y-%m-%d`/

- When you're finished, log out of the master.
- Kill the cluster.

      ~/Downloads/spark-1.1.1-bin-hadoop2.4/ec2/spark-ec2 -k laskuri destroy laskuri
