#!/bin/bash

python usage_data.py 1> usage_stdout.txt
python cost_reporting_data.py 1> cost_reporting_stdout.txt
python reports_to_bucket.py
echo "Reporting scripts done."
