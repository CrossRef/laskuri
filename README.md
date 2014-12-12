# laskuri

Log analysis for CrossRef referral data. For CrossRef Labs projects, but applicable to any DOI Registration Agency's logs. Data is research-oriented, rather than publisher-oriented.

Takes standard DOI resolution logs and aggregates to various outputs. Uses [Apache Spark](http://spark.apache.org/) framework to parallelize work.

## Input and output

The output of this program is various files placed at the output path. The output can be consumed (e.g. by the 'DOI time' project) to produce user-friendly charts, graphs, tables and chairs. The outputs are:

- all time
  - first resolution of each DOI
  - total resolutions of of each DOI
  - total referrals from each domain
  - total referrals from each subdomain (with domain)
- per year / month / day
  - total resolutions for each DOI
  - total referrals from each domain
  - total referrals from each subdomain (with domain)

You can use this locally by pointing it to a directory containing log files and a directory to place results and Spark will run as a local instance. When running on 'real' data, the input will be an Amazon S3 bucket containing all log files ever, and the output will be an S3 bucket. 

## Redaction

Some of the traffic contained in the logs is confidential, as it reveals outgoing and possibly internal traffic on Publishers' sites, which may be commercially sensitive. Because of this, referral domains and subdomains are compared to a whitelist and potentially redacted. A blacklist isn't perfect, as it requires manual intervention and limits the useful available data. Hopefully we can move to an automatically generated blacklist when we have a list of all publishers' domains. 

Every domain (and subdomain) that is redacted is given a random domain-like value, e.g. `4e05.3ecc4fb92f99.redacted`. The values are issued for the whole data processing session, so for one dataset (i.e. run of this program and its various outputs) a domain will have a consistent random value and each subdomain within it will have a consistent value. Hopefully this affords those domains sufficient privacy but allows analysis. There is no absolute guarantee that redacted domain values are unique, but they should be.

When running this locally, you can configure whether or not you want redaction. You can run with redaction into a public S3 bucket and without into a private one.

If would like a domain added to or removed from the blacklist, email jwass@crossref.org.

## Configuration and running

This program runs through the Apache Spark framework. You should understand its job configuraiotn and submission process before running this.

Because Spark has native access for various input and output mechanisms, you can easily configure it for local file system access (for development) or S3 (for production). If you supply Spark with a single file it will operate on all lines in that file. If you supply a directory, it will operate on all lines in all files in that directory, which is useful.

The following environment variables must be set: 

 - `INPUT_LOCATION`, e.g. `file:///tmp/doi_logs` or `s3://doi_logs/input`
 - `OUTPUT_LOCATION`, e.g. `file://tmp/doi_output` or `s3://doi_logs/output`
 - `REDACT`, e.g. `true` or `false`

An example `run-dev.sh.example` is included.

## Incidentally

'Laskuri' is Finnish for 'counter' apparently.

## Installation

TODO

## Usage

TODO

## Gotchas

You'll probably see this:

    WARN component.AbstractLifeCycle: FAILED org.eclipse.jetty.server.Server@15b82644: java.net.BindException: Address already in use
    java.net.BindException: Address already in use

Don't worry, it's [perfectly normal](http://community.cloudera.com/t5/Advanced-Analytics-Apache-Spark/Port-Bind-Error-in-Spark/td-p/17602).

## Options

TODO

## Examples

TODO

### TODO

 - record successful and unsuccessful resolutions
 - move from whitelist to blacklist

### Bugs

## Contact

Email jwass@crossref.org 

## License

Copyright © 2014 CrossRef

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
