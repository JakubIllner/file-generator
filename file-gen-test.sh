#!/usr/bin/bash

outfile=file-gen-test.out
logfile=file-gen-test.log

fromdate=20240901
todate=20241031
namespace=<namespace>
bucket=invoice-data
pattern='date=${date}/invoice-${timestamp}-${uuid}.json'

echo "Deleting old files"
oci os object bulk-delete -bn ${bucket} -ns ${namespace} --force

echo "Generate new files"
nohup python file-gen.py \
-s json \
-f ${fromdate} \
-t ${todate} \
-n ${namespace} \
-b ${bucket} \
-p ${pattern} \
--loglevel DEBUG \
-x 400 -y 600 \
-k 1 -l 1 \
-v 1000 -w 3000 \
> ${outfile} 2> ${logfile}&

