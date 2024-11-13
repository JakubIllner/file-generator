# __file-generator__

## Purpose

The purpose of the `file-generator` is to generate artificial structured data for testing
and store them as files (objects) in the Oracle Cloud Infrastructure (OCI) Object Storage
bucket. Files may be used for testing of data loading, data management, and data access.


## Scenarios

The `file-generator` supports the following scenarios:

* __json__ - Data is generated as JSON Lines format, with single file containing one or
more JSON documents stored as text and separated by newline.


## Data

Generated data simulate invoice documents, with products, quantities, and prices for
products or services. Example of a single document looks as follows:

```
{
  "detail": {
    "document_id": "3b1aa2e5-0cca-461f-9833-0dfc46622edf",
    "invoice_number": "M0D080HK2IGIXV2LGTGI",
    "purchase_order": "JAK70L9DC8U5L5NNKQHL",
    "contract_number": "SEOOO5CMM142YNKJAI6Q",
    "currency_code": "USD",
    "invoice_date": "2024-09-01",
    "due_date": "2024-10-31",
    "created_timestamp": "2024-09-01T05:18:51"
  },
  "customer": {
    "customer_number": "C2M37YZC5Q30SMV40DNX",
    "name": "Aczdev dzgjtkcv xcd tsqgqznvenjosrhzbbvrpqtzfbvot..",
    "addresses": [
      {
        "address_type": "BILL",
        "contact_name": "Nuwgj zaobu  yncxpvydvawcpoafyhosbendgz..",
        "address_detail": "7nq10d q4pdsacqmzwuxwr0g0rv8ohjhpbluf..",
        "zip_code": "94890",
        "city_name": "Iopypsnrpmtsze tbh zk ajqylvgadjmichzyaqij..",
        "country_name": "Portugal"
      },..
    ]
  },
  "total": {
    "base_amount": 36709085.88,
    "discount_amount": 35297658.48,
    "tax_amount": 5158312.43,
    "net_amount": 40455970.91
  },
  "tax_lines": [
    {
      "tax_code": "VAT20",
      "tax_pct": 20,
      "tax_desc": "20% VAT",
      "tax_amount": 1828868.29
    },..
  ],
  "lines": [
    {
      "line_number": 1,
      "product_code": "P1272",
      "product_desc": "P1272P1272P1272P1272P1272P1272P1272P1272P1272P1272..",
      "quantity": 365,
      "unit_price": 86.8,
      "base_amount": 31682.0,
      "discount_pct": 10,
      "discount_amount": 28513.8,
      "tax_code": "VAT20",
      "tax_pct": 20,
      "tax_amount": 5702.76,
      "net_amount": 34216.56,
      "comment": "Zmdy clwpacb roccudkba hkznnvasjkonbpupjvebjqgrf.."
    },..
  ],
  "comments": [
    {
      "comment_number": 1,
      "comment_text": "Eupbnuxkyi vlgk gukcoychcrxxojroyuxaat ijok.."
    },..
  ]
}
```


## Generator

The `file-generator` is a single threaded Python3 program `file-gen.py` that generates
data and stores them in OCI Object Storage in the format specified by parameter
`scenario`.

* The program loops over dates, starting from `fromdate` and finishing with `todate`.
* For every date, it generates one or more files. Number of files is random, between `minfiles` and `maxfiles`.
* Every file contains one or more documents. Number of documents is random, between `mindocs` and `maxdocs`.
* Size of a single document is determined by number of lines, randomly generated between `minlines` and `maxlines`.


## Object Names

Names of generated objects are created by using the `pattern` parameter and applying the
following substitutions:

* `${date}` is substituted by date in `%Y%0m%0d` format.
* `${time}` is substituted by time in `%H%M%S` format.
* `${microseconds}` is substituted by microseconds in `%f` format.
* `${timestamp}` is substituted by timestamp in `%Y%0m%0d%H%M%S%f` format.
* `${number}` is substituted by the number of file in the day (starting from 1).
* `${uuid}` is substituted by unique identifier generated as `uuid.uuid4()`.


## Parameters

The program `file-gen.py` is parameterized by the following parameters:

```
$ python file-gen.py -h
file-gen.py -s <scenario> -f <fromdate> -t <todate> -x <minfiles> -y <maxfiles> -k <mindocs> -l <maxdocs> -e <sleep> -n <namespace> -b <bucket> -p <pattern>

   Options:
   -h, --help             Print help
   -s, --scenario         Scenario (json) [mandatory]
   -f, --fromdate         Start date in YYYY-MM-DD format [mandatory]
   -t, --todate           End date in YYYY-MM-DD format [mandatory]
   -x, --minfiles         Minimum number of files in one day [1]
   -y, --maxfiles         Maximum number of files in one day [1]
   -k, --mindocs          Minimum number of documents in one file [1]
   -l, --maxdocs          Maximum number of documents in one file [1]
   -v, --minlines         Minimum number of lines in one document[100]
   -w, --maxlines         Maximum number of lines in one document[2000]
   -e, --sleep            Sleep time in seconds between files [0]
   -n, --namespace        Tenancy namespace [mandatory]
   -b, --bucket           Name of target bucket [mandatory]
   -p, --pattern          Object name pattern [mandatory]
       --loglevel         Log level [DEBUG, INFO, WARNING, ERROR, CRITICAL], default is INFO
```


## Output

The program output is a JSON document with statistics describing the generated data.

```
{
  "scenario": "json",
  "bucket": "invoice-data",
  "pattern": "date=${date}/invoice-${timestamp}-${uuid}.json",
  "start_datetime": "2024/11/11 14:58:47,430178",
  "end_datetime": "2024/11/11 15:40:49,884293",
  "elapsed_sec": 2522.454115,
  "from_date": "2024/09/01",
  "to_date": "2024/10/31",
  "day_count": 61,
  "file_count": 29799,
  "document_count": 29799,
  "line_count": 59588006,
  "size_bytes": 28946897924,
  "avg_document_size_bytes": 971405.0110406389
}

```


## Prerequisites

Before running the `file-generator`, ensure the following prerequisites are met:

* Compute instance with Python3 (tested with 3.9), OCI CLI and Python `oci` package.
* Network connectivity from the Compute instance to the Object Storage API.
* Configured `~/.oci/config` with API Key to connect to OCI API with Python SDK. Note the `file-gen.py` currently does not support instance principal authentication.


## Considerations

* You can easily modify the function `get_invoice()` to produce different types of
documents. Rest of the program does not care about the structure of the documents. In the
future, I will make the content generation function configurable.





