#!/usr/bin/python
__author__ = 'cleung'
import boto
import datetime
import os

# Filenames of reports to be uploaded
reports = []
for root, dirs, files in os.walk('reports/'):
    reports.extend(files)

s3_connect = boto.connect_s3(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])

dst_bucket = s3_connect.get_bucket("oicr.detailed.billing")
my_key = boto.s3.key.Key(dst_bucket)

def upload_one_file(report):
    my_key.key = "reports/" + str(datetime.date.today()) + "_" + report
    my_key.set_contents_from_filename("reports/" + report)

for i, report in enumerate(reports):
    print str(i+1) + "/" + str(len(reports)) + \
          ") Uploading " + report + " to S3 bucket 'oicr.detailed.billing'..."
    upload_one_file(report)
