# aws-reporting

**How to use**
* Have docker installed
* Have your aws keys as environment variables: AWS_ACCESS_KEY, AWS_SECRET_KEY. The next script will use them in docker
* Run <pre>$ bash run_docker_reporter.sh</pre>
* Goto the S3 Bucket to see the reports: https://console.aws.amazon.com/s3/home?region=us-east-1&bucket=oicr.detailed.billing&prefix=reports/

**Costs**
cost_reporting_data.py gets costs so far this month based on the most detailed billing report we have. Coming soon: functionality for selecting previous months.

**Usage**
usage_data.py shows which resources are currently live with associated KEEP- and PROD-tags

**Upload to bucket**
reports_to_bucket.py uploads whatever files were successfully generated into the S3 bucket
