"""
Generate sample invoices as JSON documents and store them in OCI Object Storage bucket.

Usage: python file-gen.py -s <scenario> -f <fromdate> -t <todate> -x <minfiles> -y <maxfiles> -k <mindocs> -l <maxdocs> -e <sleep> -n <namespace> -b <bucket> -p <pattern>

The generator will loop over dates, starting from <fromdate> and finishing with <todate>.
For every date, it will generate multiple files. Number of files is random, between <minfiles> and <maxfiles>.
Every file will have multiple JSON documents (records) separated by newline. Number of documents is random, between <mindocs> and <maxdocs>.
Size of a single JSON document is primarily determined by number of lines, randomly generated between <minlines> and <maxlines>.

Files are named according to <pattern> with the following substitutions:
${date} is substituted by date in %Y%0m%0d format
${time} is substituted by time in %H%M%S format
${microseconds} is substituted by microseconds in %f format
${timestamp} is substituted by microseconds in %Y%0m%0d%H%M%S%f format
${number} is substituted by the number of file in the day (starting from 1)
${uuid} is substituted by unique identifier generated as uuid.uuid4()
"""

import string
import time
import datetime
import random
import json
import copy
import sys
import logging
import codecs
import uuid
import os
import getopt

from dateutil.relativedelta import relativedelta
from base64 import b64encode

import oci


# ----------------------------------------------------
# SETUP FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Get input parameters
# ----------------------------------------------------
def get_input_parameters(p_argv):

   v_usage = '{} -s <scenario> -f <fromdate> -t <todate> -x <minfiles> -y <maxfiles> -k <mindocs> -l <maxdocs> -e <sleep> -n <namespace> -b <bucket> -p <pattern>'.format(p_argv[0])

   v_params = {
      'scenario':    None,
      'fromdate':    None,
      'todate':      None,
      'minfiles':    1,
      'maxfiles':    1,
      'mindocs':     1,
      'maxdocs':     1,
      'minlines':    100,
      'maxlines':    2000,
      'sleep':       0,
      'namespace':   None,
      'bucket':      None,
      'pattern':     None,
      'loglevel':    'INFO'
   } 
     
   v_help = '''
   Options:
   -h, --help             Print help
   -s, --scenario         Scenario (json) [mandatory]
   -f, --fromdate         Start date in YYYY-MM-DD format [mandatory]
   -t, --todate           End date in YYYY-MM-DD format [mandatory]
   -x, --minfiles         Minimum number of files in one day [{0}]
   -y, --maxfiles         Maximum number of files in one day [{1}]
   -k, --mindocs          Minimum number of documents in one file [{2}]
   -l, --maxdocs          Maximum number of documents in one file [{3}]
   -v, --minlines         Minimum number of lines in one document[{4}]
   -w, --maxlines         Maximum number of lines in one document[{5}]
   -e, --sleep            Sleep time in seconds between files [{6}]
   -n, --namespace        Tenancy namespace [mandatory]
   -b, --bucket           Name of target bucket [mandatory]
   -p, --pattern          Object name pattern [mandatory]
       --loglevel         Log level [DEBUG, INFO, WARNING, ERROR, CRITICAL], default is {7}
   '''.format(v_params['minfiles'], v_params['maxfiles'], v_params['mindocs'], v_params['maxdocs'], v_params['minlines'], v_params['maxlines'], v_params['sleep'], v_params['loglevel'])

   try:
      (v_opts, v_args) = getopt.getopt(p_argv[1:],"hs:f:t:x:y:k:l:v:w:e:n:b:p:",['help','scenario=','fromdate=','todate=','minfiles=','maxfiles=','mindocs=','maxdocs=','minlines=','maxlines=','sleep=','namespace=','bucket=','pattern=','loglevel='])
   except getopt.GetoptError:
      g_logger.error ('Unknown parameter or parameter with missing value')
      print (v_usage)
      sys.exit(2)
   for v_opt, v_arg in v_opts:
      if v_opt in ('-h', '--help'):
         print (v_usage)
         print (v_help)
         sys.exit()
      elif v_opt in ('-s', '--scenario'):
         v_params['scenario'] = v_arg
      elif v_opt in ('-f', '--fromdate'):
         v_params['fromdate'] = datetime.date(int(v_arg[0:4]),int(v_arg[4:6]),int(v_arg[6:8]))
      elif v_opt in ('-t', '--todate'):
         v_params['todate'] = datetime.date(int(v_arg[0:4]),int(v_arg[4:6]),int(v_arg[6:8]))
      elif v_opt in ('-x', '--minfiles'):
         v_params['minfiles'] = int(v_arg)
      elif v_opt in ('-y', '--maxfiles'):
         v_params['maxfiles'] = int(v_arg)
      elif v_opt in ('-k', '--mindocs'):
         v_params['mindocs'] = int(v_arg)
      elif v_opt in ('-l', '--maxdocs'):
         v_params['maxdocs'] = int(v_arg)
      elif v_opt in ('-v', '--minlines'):
         v_params['minlines'] = int(v_arg)
      elif v_opt in ('-w', '--maxlines'):
         v_params['maxlines'] = int(v_arg)
      elif v_opt in ('-e', '--sleep'):
         v_params['sleep'] = int(v_arg)
      elif v_opt in ('-n', '--namespace'):
         v_params['namespace'] = v_arg
      elif v_opt in ('-b', '--bucket'):
         v_params['bucket'] = v_arg
      elif v_opt in ('-p', '--pattern'):
         v_params['pattern'] = v_arg
      elif v_opt in ('--loglevel'):
         v_params['loglevel'] = v_arg.upper()

   if v_params['scenario'] == None:
      g_logger.error ('Missing value for parameter "scenario"')
      print (v_usage)
      sys.exit(2)
   elif v_params['scenario'] not in ('json'):
      g_logger.error ('Parameter "scenario" must have value "json"')
      print (v_usage)
      sys.exit(2)
   if v_params['fromdate'] == None:
      g_logger.error ('Missing value for parameter "fromdate"')
      print (v_usage)
      sys.exit(2)
   elif v_params['todate'] == None:
      g_logger.error ('Missing value for parameter "todate"')
      print (v_usage)
      sys.exit(2)
   elif v_params['minfiles'] == None:
      g_logger.error ('Missing value for parameter "minfiles"')
      print (v_usage)
      sys.exit(2)
   elif v_params['maxfiles'] == None:
      g_logger.error ('Missing value for parameter "maxfiles"')
      print (v_usage)
      sys.exit(2)
   elif v_params['mindocs'] == None:
      g_logger.error ('Missing value for parameter "mindocs"')
      print (v_usage)
      sys.exit(2)
   elif v_params['maxdocs'] == None:
      g_logger.error ('Missing value for parameter "maxdocs"')
      print (v_usage)
      sys.exit(2)
   elif v_params['minlines'] == None:
      g_logger.error ('Missing value for parameter "minlines"')
      print (v_usage)
      sys.exit(2)
   elif v_params['maxlines'] == None:
      g_logger.error ('Missing value for parameter "maxlines"')
      print (v_usage)
      sys.exit(2)
   elif v_params['sleep'] == None:
      g_logger.error ('Missing value for parameter "sleep"')
      print (v_usage)
      sys.exit(2)
   elif v_params['namespace'] == None:
      g_logger.error ('Missing value for parameter "namespace"')
      print (v_usage)
      sys.exit(2)
   elif v_params['bucket'] == None:
      g_logger.error ('Missing value for parameter "bucket"')
      print (v_usage)
      sys.exit(2)
   elif v_params['pattern'] == None:
      g_logger.error ('Missing value for parameter "pattern"')
      print (v_usage)
      sys.exit(2)
   elif v_params['loglevel'] not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
      g_logger.error ('Missing or invalid value for parameter "loglevel"')
      print (v_usage)
      sys.exit(2)

   return v_params


# ----------------------------------------------------
# Print input parameters
# ----------------------------------------------------
def print_input_parameters(p_params):

    g_logger.debug('Parameter "scenario" = {}'.format(p_params['scenario']))
    g_logger.debug('Parameter "fromdate" = {}'.format(p_params['fromdate'].strftime('%Y%0m%0d')))
    g_logger.debug('Parameter "todate" = {}'.format(p_params['todate'].strftime('%Y%0m%0d')))
    g_logger.debug('Parameter "minfiles" = {}'.format(p_params['minfiles']))
    g_logger.debug('Parameter "maxfiles" = {}'.format(p_params['maxfiles']))
    g_logger.debug('Parameter "mindocs" = {}'.format(p_params['mindocs']))
    g_logger.debug('Parameter "maxdocs" = {}'.format(p_params['maxdocs']))
    g_logger.debug('Parameter "minlines" = {}'.format(p_params['minlines']))
    g_logger.debug('Parameter "maxlines" = {}'.format(p_params['maxlines']))
    g_logger.debug('Parameter "sleep" = {}'.format(p_params['sleep']))
    g_logger.debug('Parameter "namespace" = {}'.format(p_params['namespace']))
    g_logger.debug('Parameter "bucket" = {}'.format(p_params['bucket']))
    g_logger.debug('Parameter "pattern" = {}'.format(p_params['pattern']))
    g_logger.debug('Parameter "loglevel" = {}'.format(p_params['loglevel']))



# ----------------------------------------------------
# Initialize logging
# ----------------------------------------------------
def initialize_logging (p_logger_name, p_level):

    if p_level == None:
        v_level = logging.DEBUG
    elif p_level == "DEBUG":
        v_level = logging.DEBUG
    elif p_level == "INFO":
        v_level = logging.INFO
    elif p_level == "WARNING":
        v_level = logging.WARNING
    elif p_level == "ERROR":
        v_level = logging.ERROR
    elif p_level == "CRITICAL":
        v_level = logging.CRITICAL
    else:
        v_level = logging.DEBUG

    global g_logger
    g_logger = logging.getLogger(p_logger_name)
    g_logger.setLevel(v_level)
    v_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s', datefmt='%Y/%m/%d %H:%M:%S', style='%')
    v_handler = logging.StreamHandler()
    v_handler.setFormatter(v_formatter)
    g_logger.addHandler(v_handler)


# ----------------------------------------------------
# CONTENT GENERATION FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Get random string
# ----------------------------------------------------
def get_random_string(p_choices, p_min_length, p_max_length):
    return ''.join(random.choices(p_choices, k=random.randrange(p_min_length,p_max_length+1)))


# ----------------------------------------------------
# Get random integer
# ----------------------------------------------------
def get_random_integer(p_min_length, p_max_length):
    return random.randrange(p_min_length,p_max_length+1)


# ----------------------------------------------------
# Get random number
# ----------------------------------------------------
def get_random_number(p_min, p_max, p_digits):
    return round(random.uniform(p_min,p_max),p_digits)


# ----------------------------------------------------
# Get random timestamp
# ----------------------------------------------------
def get_random_timestamp(p_date):
    return datetime.datetime(year=p_date.year,month=p_date.month,day=p_date.day,hour=get_random_integer(0,23),minute=get_random_integer(0,59),second=get_random_integer(0,59))


# ----------------------------------------------------
# Get random crdr code
# ----------------------------------------------------
def get_random_crdr():
    return random.choices(['CR','DR'],k=1)[0]


# ----------------------------------------------------
# Get random currency code
# ----------------------------------------------------
def get_random_currency_code():
    return random.choices(['EUR','EUR','EUR','USD','USD','USD','GBP','CHF','JPY'],k=1)[0]


# ----------------------------------------------------
# Get random country
# ----------------------------------------------------
def get_random_country():
    return random.choices(['France','Italy','Spain','Germany','Netherlands','Belgium','Switzerland','Portugal','Poland','Norway','Denmark','Sweden','Finland','Czechia','Austria','United Kingdom','United States','Japan'],k=1)[0]


# ----------------------------------------------------
# Get random tax
# ----------------------------------------------------
def get_random_tax():
    v_tax_pct = random.choices([0,15,20,25],k=1)[0]
    v_tax_code = 'VAT' + str(v_tax_pct)
    v_tax_desc = str(v_tax_pct) + '%' + ' VAT'
    return (v_tax_pct, v_tax_code, v_tax_desc)


# ----------------------------------------------------
# Get random discount
# ----------------------------------------------------
def get_random_discount():
    v_discount_pct = random.choices([0,0,0,0,0,0,0,0,0,0,5,5,10,15,20],k=1)[0]
    return v_discount_pct


# ----------------------------------------------------
# Get random product
# ----------------------------------------------------
def get_random_product():
    v_product_number = get_random_integer(1,2000)
    v_product_code = 'P{0:04}'.format(v_product_number)
    v_product_desc = v_product_code * 20
    return (v_product_number, v_product_code, v_product_desc)


# ----------------------------------------------------
# Generate invoice
# ----------------------------------------------------
def get_invoice(p_date, p_minlines, p_maxlines):

    # initialize
    v_total_base_amount = 0
    v_total_discount_amount = 0
    v_total_tax_amount = 0
    v_total_net_amount = 0
    v_invoice_date = p_date
    v_due_date = v_invoice_date + datetime.timedelta(days=60)
    v_tax_lines = {}

    # generate lines
    v_lines = []
    v_line_count = get_random_integer(p_minlines, p_maxlines)

    for v_line_number in range(1,v_line_count+1):

        # generate line
        (v_product_number, v_product_code, v_product_desc) = get_random_product()
        (v_tax_pct, v_tax_code, v_tax_desc) = get_random_tax()

        v_line_quantity = get_random_integer(1,1000)
        v_line_unit_price = get_random_number(0.5,100,2)
        v_line_discount_pct = get_random_discount()
        v_line_base_amount = round(v_line_quantity * v_line_unit_price,2)
        v_line_discount_amount = round(v_line_base_amount * (100-v_line_discount_pct)/100,2)
        v_line_tax_amount = round(v_line_discount_amount * v_tax_pct/100,2)
        v_line_net_amount = v_line_discount_amount + v_line_tax_amount

        v_line = {
            "line_number": v_line_number,
            "product_code": v_product_code,
            "product_desc": v_product_desc,
            "quantity": v_line_quantity,
            "unit_price": v_line_unit_price,
            "base_amount": v_line_base_amount,
            "discount_pct": v_line_discount_pct,
            "discount_amount": v_line_discount_amount,
            "tax_code": v_tax_code,
            "tax_pct": v_tax_pct,
            "tax_amount": v_line_tax_amount,
            "net_amount": v_line_net_amount,
            "comment": get_random_string(string.ascii_lowercase+' ',20,200).strip().capitalize()
        }

        v_lines.append(v_line)

        # save totals
        v_total_base_amount = v_total_base_amount + v_line_base_amount 
        v_total_discount_amount = v_total_discount_amount + v_line_discount_amount
        v_total_tax_amount = v_total_tax_amount + v_line_tax_amount 
        v_total_net_amount = v_total_net_amount + v_line_net_amount 

        # save taxes
        if v_tax_code not in v_tax_lines:
            v_tax_lines[v_tax_code] = {"tax_code": v_tax_code, "tax_pct": v_tax_pct, "tax_desc": v_tax_desc, "tax_amount": v_line_tax_amount}
        else:
            v_tax_lines[v_tax_code] = {"tax_code": v_tax_code, "tax_pct": v_tax_pct, "tax_desc": v_tax_desc, "tax_amount": v_tax_lines[v_tax_code]["tax_amount"] + v_line_tax_amount}

    # generate tax lines
    v_tax_array = []
    for v_tax_code in v_tax_lines:
        v_tax_array.append(v_tax_lines[v_tax_code])

    # generate comments
    v_comments = []
    v_comment_count = get_random_integer(1, 10)

    for v_comment_number in range(1,v_comment_count+1):

        v_comment = {
            "comment_number": v_comment_number,
            "comment_text": get_random_string(string.ascii_lowercase+' ',20,200).strip().capitalize()
        }

        v_comments.append(v_comment)

    # generate invoice
    v_invoice = {
        "detail": {
            "document_id": str(uuid.uuid4()),
            "invoice_number": get_random_string(string.ascii_uppercase+string.digits,20,20),
            "purchase_order": get_random_string(string.ascii_uppercase+string.digits,20,20),
            "contract_number": get_random_string(string.ascii_uppercase+string.digits,20,20),
            "currency_code": get_random_currency_code(),
            "invoice_date": v_invoice_date.isoformat(),
            "due_date": v_due_date.isoformat(),
            "created_timestamp": get_random_timestamp(v_invoice_date).isoformat(),
        },
        "customer": {
            "customer_number": get_random_string(string.ascii_uppercase+string.digits,20,20),
            "name": get_random_string(string.ascii_lowercase+' ',20,200).strip().capitalize(),
            "addresses": [
                {
                    "address_type": "BILL",
                    "contact_name": get_random_string(string.ascii_lowercase+' ',20,120).strip().capitalize(),
                    "address_detail": get_random_string(string.ascii_lowercase+string.digits+' ',20,200).strip().capitalize(),
                    "zip_code": str(get_random_integer(10000,99999)),
                    "city_name": get_random_string(string.ascii_lowercase+' ',20,100).strip().capitalize(),
                    "country_name": get_random_country()
                },
                {
                    "address_type": "SHIP",
                    "contact_name": get_random_string(string.ascii_lowercase+' ',20,120).strip().capitalize(),
                    "address_detail": get_random_string(string.ascii_lowercase+string.digits+' ',20,200).strip().capitalize(),
                    "zip_code": str(get_random_integer(10000,99999)),
                    "city_name": get_random_string(string.ascii_lowercase+' ',20,100).strip().capitalize(),
                    "country_name": get_random_country()
                }
            ]
        },
        "total": {
            "base_amount": round(v_total_base_amount,2),
            "discount_amount": round(v_total_discount_amount,2),
            "tax_amount": round(v_total_tax_amount,2),
            "net_amount": round(v_total_net_amount,2)
        },
        "tax_lines": v_tax_array,
        "lines": v_lines,
        "comments": v_comments
    }

    return v_invoice, v_line_count


# ----------------------------------------------------
# Get content
# ----------------------------------------------------
def get_content(p_params, p_date):

    v_content = ''
    v_document_counter = 0
    v_line_counter = 0

    v_record_count = get_random_integer(p_params['mindocs'], p_params['maxdocs'])

    for v_current_record in range(1,v_record_count+1):

        (v_record, v_line_count) = get_invoice(p_date, p_params['minlines'], p_params['maxlines'])

        if (v_current_record > 1):
            v_content = v_content + "\n"

        v_content = v_content + json.dumps(v_record)
        v_document_counter = v_document_counter + 1
        v_line_counter = v_line_counter + v_line_count

    return v_content, v_document_counter, v_line_counter, len(v_content)


# ----------------------------------------------------
# STORAGE FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Get file name
# ----------------------------------------------------
def get_file_name(p_params, p_date, p_files_count, p_current_file):

    v_current_timestamp = datetime.datetime.now()
    v_file_name = p_params['pattern']
    v_file_name = v_file_name.replace('${date}',p_date.strftime('%Y%0m%0d'))
    v_file_name = v_file_name.replace('${time}',v_current_timestamp.strftime('%H%M%S'))
    v_file_name = v_file_name.replace('${microseconds}',v_current_timestamp.strftime('%f'))
    v_file_name = v_file_name.replace('${timestamp}',p_date.strftime('%Y%0m%0d')+v_current_timestamp.strftime('%H%M%S%f'))
    v_file_name = v_file_name.replace('${number}',str(p_current_file))
    v_file_name = v_file_name.replace('${uuid}',str(uuid.uuid4()))

    return v_file_name


# ----------------------------------------------------
# Get object storage client
# ----------------------------------------------------
def get_object_storage_client():

    v_oci_config = oci.config.from_file('~/.oci/config', 'DEFAULT')
    v_oci_object_storage_client = oci.object_storage.ObjectStorageClient(config=v_oci_config)

    return v_oci_object_storage_client


# ----------------------------------------------------
# Write file to object storage
# ----------------------------------------------------
def write_file_to_object_storage(p_content, p_client, p_namespace, p_bucket, p_object_name):

    try:
        v_content_length = len(p_content)
        v_response = p_client.put_object(
            namespace_name = p_namespace,
            bucket_name = p_bucket,
            object_name = p_object_name,
            put_object_body = p_content,
            content_length = v_content_length,
            content_type = 'application/json',
            content_disposition = 'attachment'
        )
    except oci.exceptions.ServiceError as e:
        g_logger.error ('Writing object failed with status {}, code {}, message {}'.format(e.status, e.code, e.message))
        raise

    return (v_response, v_content_length)


# ----------------------------------------------------
# MAIN FUNCTION
# ----------------------------------------------------
def main(p_argv):

    # Initialize
    initialize_logging(p_argv[0], 'INFO');
    v_timestamp = {"start_datetime": datetime.datetime.now()}
    v_day_counter = 0
    v_file_counter = 0
    v_document_counter = 0
    v_line_counter = 0
    v_size_counter = 0

    # Get command line parameters
    v_params = get_input_parameters(p_argv)
    g_logger.setLevel(v_params['loglevel'])
    print_input_parameters(v_params)

    # Get object storage client
    v_oci_object_storage_client = get_object_storage_client()

    # Loop over dates
    v_current_date = v_params['fromdate']
    while v_current_date <= v_params['todate']:
        g_logger.debug ('Processing date {0}'.format(v_current_date.strftime('%Y%0m%0d')))

        # Loop over files in the day
        v_files_count = get_random_integer(v_params['minfiles'], v_params['maxfiles'])
        for v_current_file in range(1,v_files_count+1):

            # Get file name
            v_file_name = get_file_name(v_params,v_current_date,v_files_count,v_current_file)
            g_logger.debug ('Generating file {0}'.format(v_file_name))

            # Get content
            (v_content, v_document_count, v_line_count, v_size_bytes) = get_content(v_params, v_current_date)

            # Write content
            if v_params['scenario'] == 'json':
                (v_response, v_content_length) = write_file_to_object_storage(
                     p_content = v_content,
                     p_client = v_oci_object_storage_client,
                     p_namespace = v_params['namespace'],
                     p_bucket = v_params['bucket'],
                     p_object_name = v_file_name
                )

            # Update statistics
            v_file_counter = v_file_counter + 1
            v_document_counter = v_document_counter + v_document_count
            v_line_counter = v_line_counter + v_line_count
            v_size_counter = v_size_counter + v_size_bytes

            # Sleep between files
            time.sleep(v_params['sleep'])
        
        # Go to the next day
        v_current_date = v_current_date + datetime.timedelta(days=1)
        v_day_counter = v_day_counter + 1

    # Print statistics
    v_timestamp["end_datetime"] = datetime.datetime.now()

    if v_file_counter > 0:
        avg_document_size_bytes = round(v_size_counter/v_file_counter,2)
    else:
        avg_document_size_bytes = 0

    v_results = {
        "scenario": v_params["scenario"],
        "bucket": v_params["bucket"],
        "pattern": v_params["pattern"],
        "start_datetime": v_timestamp["start_datetime"].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
        "end_datetime": v_timestamp["end_datetime"].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
        "elapsed_sec": (v_timestamp['end_datetime'] - v_timestamp['start_datetime']).total_seconds(),
        "from_date": v_params["fromdate"].strftime('%Y/%0m/%0d'),
        "to_date": v_params["todate"].strftime('%Y/%0m/%0d'),
        "day_count": v_day_counter,
        "file_count": v_file_counter,
        "document_count": v_document_counter,
        "line_count": v_line_counter,
        "size_bytes": v_size_counter,
        "avg_document_size_bytes": avg_document_size_bytes
    }

    print(json.dumps(v_results))


# ----------------------------------------------------
# Call the main function
# ----------------------------------------------------
main(sys.argv)


# ----------------------------------------------------
# ----------------------------------------------------
# ----------------------------------------------------


