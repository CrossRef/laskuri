# laskuri

Log analysis for CrossRef referral data. For CrossRef Labs projects, but applicable to any DOI Registration Agency's logs. Data is research-oriented, rather than publisher-oriented.

Takes standard DOI resolution logs and aggregates to various outputs. Uses [Apache Spark](http://spark.apache.org/) framework to parallelize work.

## Quick start

Run:

    ./scripts-example/run-dev-lein.sh

This will take the example file at `example/hundred/input/hundred.txt` and write the output to `example/hundred/output`.

## Input and output

The output of this program is various files placed at the output path. The output can be consumed (e.g. by the 'DOI time' project) to produce user-friendly charts, graphs, tables and chairs. The outputs are:

For the given input:

- all time
  - first resolution of each DOI, `ever-doi-first-date`
  - total resolutions of of each DOI, `ever-doi-count`
  - total referrals from each domain, `ever-domain-count`
  - total referrals from each subdomain (with domain), `ever-subdomain-count`
- per period (year, month, day)
  - total resolutions for each DOI, `(year|month|day)-doi-periods-count`
  - total referrals from each domain, `(year|month|day)-domain-periods-count`
  - total referrals from each subdomain (with domain),  `(year|month|day)-subdomain-periods-count`
  - top 100 domains (i.e. monthly popularity contest),  `(year|month|day)-top-domains`

You can use this locally by pointing it to a directory containing log files and a directory to place results and Spark will run as a local instance. When running on 'real' data, the input will be an Amazon S3 bucket containing all log files ever, and the output will be an S3 bucket. 

### Formats:

- `host` is, e.g. `www.example.com` including subdomain
- `domain` is, e.g. `example.
- dates are ISO8601
- for 'per period' stats, the date corresponds to the first instant of that period (day, month, year)

Lines of:

#### `ever-doi-first-date`

    «doi»\t«date»

#### `ever-doi-count`

    «doi»\t«count»

#### `ever-domain-count`


    «host»\t«count»

#### `ever-subdomain-count`

    «host»\t«domain»\t«count»

#### `(year|month|day)-doi-periods-count`
    
    «doi»\t(«date»\t«count»\t)*

#### `(year|month|day)-domain-periods-count`
    
    «doi»\t(«host»\t«count»\t)*

#### `(year|month|day)-subdomain-periods-count`
    
    «doi»\t(«host»\t«domain»\t«count»\t)*

#### `(year|month|day)-top-domains`
    
    «date»\t(«host»\t«count»\t)*


## Redaction

Some of the traffic contained in the logs is confidential, as it reveals outgoing and possibly internal traffic on Publishers' sites, which may be commercially sensitive. Because of this, referral domains and subdomains are compared to a whitelist and potentially redacted. A blacklist isn't perfect, as it requires manual intervention and limits the useful available data. Hopefully we can move to an automatically generated blacklist when we have a list of all publishers' domains. 

Every domain (and subdomain) that is redacted is given a random domain-like value, e.g. `4e05.3ecc4fb92f99.redacted`. The values are issued for the whole data processing session, so for one dataset (i.e. run of this program and its various outputs) a domain will have a consistent random value and each subdomain within it will have a consistent value. Hopefully this affords those domains sufficient privacy but allows analysis. There is no absolute guarantee that redacted domain values are unique, but they should be.

When running this locally, you can configure whether or not you want redaction. You can run with redaction into a public S3 bucket and without into a private one.

If would like a domain added to or removed from the blacklist, email jwass@crossref.org.

## Configuration and running

This program runs through the Apache Spark framework. You should understand its job configuration and submission process before running this.

Because Spark has native access for various input and output mechanisms, you can easily configure it for local file system access (for development) or S3 (for production). If you supply Spark with a single file it will operate on all lines in that file. If you supply a directory, it will operate on all lines in all files in that directory, which is useful.

The following environment variables must be set: 

 - `INPUT_LOCATION`, e.g. `file:///tmp/doi_logs` (local) or `s3://doi_logs/input` (S3 - don't) or `/input` (for HDFS)
 - `OUTPUT_LOCATION`, e.g. `file://tmp/doi_output` or (local) `s3://doi_logs/output` (as above) or `/output` (for HDFS)
 - `REDACT`, e.g. `true` or not
 - `DEV_LOCAL`, e.g. `true` or not - if not set, inherit config from spark runner. If set, use 'local' spark master.
 - `ALLTIME`, e.g. `true` or not - generate all-time stats (`ever`)
 - `YEAR`, e.g. `true` or not - generate per-year period stats
 - `MONTH`, e.g. `true` or not - generate per-month period stats
 - `DAY`, e.g. `true` or not - generate per-day period stats

### Leiningen

The project can be run with `lein run` during development. An example `scripts-example/run-dev-lein.sh` is included. 

### spark-submit

It can be run by `spark-submit` in production. To do this:

    lein uberjar
    spark-submit --class laskuri.core --name "Laskuri" --master local ./target/uberjar/laskuri-0.1.0-SNAPSHOT-standalone.jar

Example given in `example/run-example-s3.sh`.

### On AWS

This project is all geared up to run over lots of logs files on AWS. See [running_on_aws.md](./doc/running_on_aws.md).

## Amazon S3

S3 is used to store the log files, as it's cheap, resilient and accessible from Elastic Compute instances for free. It's [next to useless to use directly though](http://wiki.apache.org/hadoop/AmazonS3), so it's far better to copy files locally (or to HDFS) before using. But if you wanted to, you could (see config above).

As of the new [AWS Signature Version 4](http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html), authentication changes (which is optional in some zones and compusory, e.g. in Frankfurt) mean that [Hadoop is now incompatible until this fix](https://issues.apache.org/jira/browse/JCLOUDS-480) is deployed. So if authentication fails, try a bucket in a different availability zone.

## Incidentally

'Laskuri' is Finnish for 'counter' apparently.


## Gotchas

You'll probably see this:

    WARN component.AbstractLifeCycle: FAILED org.eclipse.jetty.server.Server@15b82644: java.net.BindException: Address already in use
    java.net.BindException: Address already in use

Don't worry, it's [perfectly normal](http://community.cloudera.com/t5/Advanced-Analytics-Apache-Spark/Port-Bind-Error-in-Spark/td-p/17602).

## Contact

Email jwass@crossref.org 

## License

Copyright © 2014 CrossRef

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
