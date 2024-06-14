"""
Generate and test parallel data ingestion workload into Oracle Database and other Oracle Cloud services such as Streaming.

Usage: python run-gen.py -s <scenario> -z <size> -t <threads> -d <duration> -x <min records> -y <max records> -i <iterations> -e <sleep> -b <table> -u <dbuser> -p <dbpwd> -c <dbconnect> -o <topic ocid>
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

import oracledb
import oci

from concurrent.futures import ProcessPoolExecutor


# ----------------------------------------------------
# SETUP FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Get input parameters
# ----------------------------------------------------
def get_input_parameters(p_argv):

   v_usage = '{} -s <scenario> -z <size> -t <threads> -d <duration> -x <min records> -y <max records> -i <iterations> -e <sleep> -b <table> -u <dbuser> -p <dbpwd> -c <dbconnect> -o <topic ocid>'.format(p_argv[0])

   v_params = {
      'scenario':    None,
      'size':        None,
      'threads':     None,
      'thread':      None,
      'duration':    None,
      'minrec':      60,
      'maxrec':      100,
      'iterations':  1,
      'sleep':       0,
      'table':       None,
      'dbuser':      None,
      'dbpwd':       None,
      'dbconnect':   None,
      'topic':       None,
      'loglevel':    'INFO'
   } 
     
   v_help = '''
   Options:
   -h, --help             Print help
   -s, --scenario         Scenario (single, batch, array, fast, stream) [mandatory]
   -z, --size             Size of database or streaming instance [mandatory]
   -t, --threads          Number of threads [mandatory]
   -d, --duration         Duration in seconds [mandatory]
   -x, --minrec           Minimum number of records in iteration [{0}]
   -y, --maxrec           Maximum number of records in iteration [{1}]
   -i, --iterations       Number of iterations before write to database [{2}]
   -e, --sleep            Sleep time in seconds between iterations [{3}]
   -b, --table            Name of target table [mandatory if scenario=single|batch|array|fast]
   -u, --dbuser           Database user [mandatory if scenario=single|batch|array|fast]
   -p, --dbpwd            Database user password [mandatory if scenario=single|batch|array|fast]
   -c, --dbconnect        Database connect string [mandatory if scenario=single|batch|array|fast]
   -o, --topic            Streaming topic OCID [mandatory if scenario=stream]
       --loglevel         Log level [DEBUG, INFO, WARNING, ERROR, CRITICAL], default is {4}
   '''.format(v_params['minrec'], v_params['maxrec'], v_params['iterations'], v_params['sleep'], v_params['loglevel'])

   try:
      (v_opts, v_args) = getopt.getopt(p_argv[1:],"hs:z:t:d:x:y:i:e:b:u:p:c:o:",['help','scenario=','size=','threads=','duration=','minrec=','maxrec=','iterations=','sleep=','table=','dbuser=','dbpwd=','dbconnect=','topic=','loglevel='])
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
      elif v_opt in ('-z', '--size'):
         v_params['size'] = v_arg
      elif v_opt in ('-t', '--threads'):
         v_params['threads'] = int(v_arg)
      elif v_opt in ('-d', '--duration'):
         v_params['duration'] = int(v_arg)
      elif v_opt in ('-x', '--minrec'):
         v_params['minrec'] = int(v_arg)
      elif v_opt in ('-y', '--maxrec'):
         v_params['maxrec'] = int(v_arg)
      elif v_opt in ('-i', '--iterations'):
         v_params['iterations'] = int(v_arg)
      elif v_opt in ('-e', '--sleep'):
         v_params['sleep'] = int(v_arg)
      elif v_opt in ('-b', '--table'):
         v_params['table'] = v_arg
      elif v_opt in ('-u', '--dbuser'):
         v_params['dbuser'] = v_arg
      elif v_opt in ('-p', '--dbpwd'):
         v_params['dbpwd'] = v_arg
      elif v_opt in ('-c', '--dbconnect'):
         v_params['dbconnect'] = v_arg
      elif v_opt in ('-o', '--topic'):
         v_params['topic'] = v_arg
      elif v_opt in ('--loglevel'):
         v_params['loglevel'] = v_arg.upper()

   if v_params['scenario'] == None:
      g_logger.error ('Missing value for parameter "scenario"')
      print (v_usage)
      sys.exit(2)
   elif v_params['scenario'] not in ('single', 'batch', 'array', 'fast', 'stream'):
      g_logger.error ('Parameter "scenario" must have value "single", "batch", "array", "fast", or "stream"')
      print (v_usage)
      sys.exit(2)
   if v_params['size'] == None:
      g_logger.error ('Missing value for parameter "size"')
      print (v_usage)
      sys.exit(2)
   elif v_params['threads'] == None:
      g_logger.error ('Missing value for parameter "threads"')
      print (v_usage)
      sys.exit(2)
   elif v_params['duration'] == None:
      g_logger.error ('Missing value for parameter "duration"')
      print (v_usage)
      sys.exit(2)
   elif v_params['minrec'] == None:
      g_logger.error ('Missing value for parameter "minrec"')
      print (v_usage)
      sys.exit(2)
   elif v_params['maxrec'] == None:
      g_logger.error ('Missing value for parameter "maxrec"')
      print (v_usage)
      sys.exit(2)
   elif v_params['iterations'] == None:
      g_logger.error ('Missing value for parameter "iterations"')
      print (v_usage)
      sys.exit(2)
   elif v_params['sleep'] == None:
      g_logger.error ('Missing value for parameter "sleep"')
      print (v_usage)
      sys.exit(2)
   elif v_params['table'] == None and v_params['scenario'] in ('single', 'batch', 'array', 'fast'):
      g_logger.error ('Missing value for parameter "table"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbuser'] == None and v_params['scenario'] in ('single', 'batch', 'array', 'fast'):
      g_logger.error ('Missing value for parameter "dbuser"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbpwd'] == None and v_params['scenario'] in ('single', 'batch', 'array', 'fast'):
      g_logger.error ('Missing value for parameter "dbpwd"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbconnect'] == None and v_params['scenario'] in ('single', 'batch', 'array', 'fast'):
      g_logger.error ('Missing value for parameter "dbconnect"')
      print (v_usage)
      sys.exit(2)
   elif v_params['topic'] == None and v_params['scenario'] == ('stream'):
      g_logger.error ('Missing value for parameter "topic"')
      print (v_usage)
      sys.exit(2)
   elif v_params['loglevel'] not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
      g_logger.error ('Missing or invalid value for parameter "loglevel"')
      print (v_usage)
      sys.exit(2)

   return v_params


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
# Get random timestamp
# ----------------------------------------------------
def get_random_timestamp(p_date):
    return datetime.datetime(year=p_date.year,month=p_date.month,day=p_date.day,hour=get_random_integer(0,23),minute=get_random_integer(0,59),second=get_random_integer(0,59))


# ----------------------------------------------------
# Get random currency code
# ----------------------------------------------------
def get_random_crdr():
    return random.choices(['CR','DR'],k=1)[0]


# ----------------------------------------------------
# Get random currency code
# ----------------------------------------------------
def get_random_currency_code():
    return random.choices(['EUR','EUR','EUR','USD','USD','USD','GBP','CHF','JPY'],k=1)[0]


# ----------------------------------------------------
# Generate journal lines
# ----------------------------------------------------
def get_journal_lines(p_minrec, p_maxrec):

    v_journal_lines = []

    v_line_count = get_random_integer(p_minrec, p_maxrec)
    v_entered_debit_amount_sum = 0
    v_entered_credit_amount_sum = 0
    v_current_datetime = datetime.datetime.today()
    v_posted_date = v_current_datetime.replace(microsecond=0, second=0, minute=0, hour=0)
    v_period_date = v_posted_date + relativedelta(months=1, day=1, days=-1)

    v_journal_header_source_code               = get_random_string(string.ascii_uppercase,3,3)
    v_journal_external_reference               = get_random_string(string.ascii_uppercase+string.digits,20,20)
    v_journal_header_description               = get_random_string(string.ascii_lowercase,10,80)
    v_period_code                              = '{0:04d}{1:02d}'.format(v_posted_date.year,v_posted_date.month)
    v_period_date                              = v_period_date.isoformat()
    v_currency_code                            = get_random_currency_code()
    v_journal_category_code                    = get_random_string(string.ascii_uppercase,3,3)
    v_journal_posted_date                      = v_posted_date.isoformat()
    v_journal_created_date                     = v_current_datetime.replace(microsecond=0, second=0, minute=0, hour=0).isoformat()
    v_journal_created_timestamp                = v_current_datetime.isoformat()
    v_journal_actual_flag                      = 'Y'
    v_journal_status                           = get_random_string(string.ascii_uppercase,3,3)
    v_journal_header_name                      = get_random_string(string.ascii_lowercase,10,40)
    v_reversal_flag                            = 'N'
    v_reversal_journal_header_source_code      = ''

    for v_line_number in range(1,v_line_count+1):
    
        v_journal_line_number                  = v_line_number
        v_account_code                         = 'A'+get_random_string(string.digits,3,3)
        v_organization_code                    = 'R'+get_random_string(string.digits,3,3)
        v_project_code                         = 'P'+get_random_string(string.digits,3,3)
        v_journal_line_description             = get_random_string(string.ascii_lowercase,10,80)

        if v_line_number < v_line_count:

            v_journal_line_type                = get_random_crdr()
            if v_journal_line_type == 'CR':
                v_entered_debit_amount         = 0
                v_entered_credit_amount        = random.uniform(1,10000)
                v_accounted_debit_amount       = v_entered_debit_amount
                v_accounted_credit_amount      = v_entered_credit_amount
                v_entered_credit_amount_sum    = v_entered_credit_amount_sum + v_entered_credit_amount
            else:
                v_entered_debit_amount         = random.uniform(1,10000)
                v_entered_credit_amount        = 0
                v_accounted_debit_amount       = v_entered_debit_amount
                v_accounted_credit_amount      = v_entered_credit_amount
                v_entered_debit_amount_sum     = v_entered_debit_amount_sum + v_entered_debit_amount

        else:

            if v_entered_debit_amount_sum > v_entered_credit_amount_sum:
                v_journal_line_type            = 'CR'
                v_entered_debit_amount         = 0
                v_entered_credit_amount        = v_entered_debit_amount_sum - v_entered_credit_amount_sum
                v_accounted_debit_amount       = v_entered_debit_amount
                v_accounted_credit_amount      = v_entered_credit_amount
            else:
                v_journal_line_type            = 'DR'
                v_entered_debit_amount         = v_entered_credit_amount_sum - v_entered_debit_amount_sum
                v_entered_credit_amount        = 0
                v_accounted_debit_amount       = v_entered_debit_amount
                v_accounted_credit_amount      = v_entered_credit_amount

        v_journal_lines.append({
            'journal_header_source_code'           : v_journal_header_source_code,
            'journal_external_reference'           : v_journal_external_reference,
            'journal_header_description'           : v_journal_header_description,
            'period_code'                          : v_period_code,
            'period_date'                          : v_period_date,
            'currency_code'                        : v_currency_code,
            'journal_category_code'                : v_journal_category_code,
            'journal_posted_date'                  : v_journal_posted_date,
            'journal_created_date'                 : v_journal_created_date,
            'journal_created_timestamp'            : v_journal_created_timestamp,
            'journal_actual_flag'                  : v_journal_actual_flag,
            'journal_status'                       : v_journal_status,
            'journal_header_name'                  : v_journal_header_name,
            'reversal_flag'                        : v_reversal_flag,
            'reversal_journal_header_source_code'  : v_reversal_journal_header_source_code,
            'journal_line_number'                  : v_journal_line_number,
            'account_code'                         : v_account_code,
            'organization_code'                    : v_organization_code,
            'project_code'                         : v_project_code,
            'journal_line_type'                    : v_journal_line_type,
            'entered_debit_amount'                 : v_entered_debit_amount,
            'entered_credit_amount'                : v_entered_credit_amount,
            'accounted_debit_amount'               : v_accounted_debit_amount,
            'accounted_credit_amount'              : v_accounted_credit_amount,
            'journal_line_description'             : v_journal_line_description
        })

    return v_journal_lines


# ----------------------------------------------------
# Generate array of journals
# ----------------------------------------------------
def get_journals(p_journal_count, p_minrec, p_maxrec):

    v_journal_lines = []

    for v_journal_number in range(1,p_journal_count+1):
        v_journal_lines = v_journal_lines + get_journal_lines(p_minrec, p_maxrec)

    return v_journal_lines


# ----------------------------------------------------
# CONNECT FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Normal connect to database
# ----------------------------------------------------
def connect_oracle(p_params):

    v_context = {
       'connection':  None,
       'cursor':      None
    } 

    oracledb.init_oracle_client()

    try:
        v_context['connection'] = oracledb.connect(
            user=p_params['dbuser'],
            password=p_params['dbpwd'],
            dsn=p_params['dbconnect']
        )
        v_context['cursor'] = v_context['connection'].cursor()

    except Exception as e:
        g_logger.warning ('Cannot connect to the database: {0}'.format(e))
        raise

    return v_context


# ----------------------------------------------------
# Connect to database for fast ingest
# ----------------------------------------------------
def connect_oracle_fast(p_params):

    v_context = {
       'connection':  None,
       'cursor':      None
    } 

    oracledb.init_oracle_client()

    try:
        v_context['connection'] = oracledb.connect(
            user=p_params['dbuser'],
            password=p_params['dbpwd'],
            dsn=p_params['dbconnect']
        )
        v_context['cursor'] = v_context['connection'].cursor()
        v_context['cursor'].execute('alter session set optimizer_ignore_hints = false')

    except Exception as e:
        g_logger.warning ('Cannot connect to the database for fast ingest: {0}'.format(e))
        raise

    return v_context


# ----------------------------------------------------
# Connect to streaming
# ----------------------------------------------------
def connect_streaming(p_params):

    v_context = {
       'config':       None,
       'adminclient':  None,
       'stream':       None,
       'streamclient': None
    } 

    try:
        v_context['config'] = oci.config.from_file()
    except Exception as e:
        g_logger.warning ('Cannot read OCI config default file: {0}'.format(e))
        raise

    try:
        v_context['adminclient'] = oci.streaming.StreamAdminClient(config=v_context['config'])
    except Exception as e:
        g_logger.warning ('Cannot get StreamAdminClient: {0}'.format(e))
        raise

    try:
        v_context['stream'] = v_context['adminclient'].get_stream(stream_id=p_params['topic']).data
    except Exception as e:
        g_logger.warning ('Cannot get stream: {0}'.format(e))
        raise

    try:
       v_context['streamclient'] = oci.streaming.StreamClient(config=v_context['config'], service_endpoint=v_context['stream'].messages_endpoint)
    except Exception as e:
        g_logger.warning ('Cannot get StreamClient: {0}'.format(e))
        raise

    return v_context


# ----------------------------------------------------
# RUN FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Run with commit after every row
# ----------------------------------------------------
def run_single(p_params, p_context, p_scenario_name, p_run_id):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        g_logger.warning ('Data generator failed with exception: {0}'.format(e))
        raise

    v_data_count = 0
    v_failure_count = 0
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])
    v_data_size = 0

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)
        v_data_size = v_data_size + (len(v_data_line_json)+len(v_uuid)+len(str(v_timestamp)))

        p_context['cursor'].setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
        p_context['cursor'].execute(v_sql, ts=v_timestamp, id=v_uuid, scenario=p_scenario_name, run_id=p_run_id, payload=v_data_line_json)
        p_context['connection'].commit()

        v_data_count = v_data_count+1

    return v_data_count, v_failure_count, v_data_size, v_data_size


# ----------------------------------------------------
# Run with commit after the batch
# ----------------------------------------------------
def run_batch(p_params, p_context, p_scenario_name, p_run_id):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        g_logger.warning ('Data generator failed with exception: {0}'.format(e))
        raise

    v_data_count = 0
    v_failure_count = 0
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])
    v_data_size = 0

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)
        v_data_size = v_data_size + (len(v_data_line_json)+len(v_uuid)+len(str(v_timestamp)))

        p_context['cursor'].setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
        p_context['cursor'].execute(v_sql, ts=v_timestamp, id=v_uuid, scenario=p_scenario_name, run_id=p_run_id, payload=v_data_line_json)

        v_data_count = v_data_count+1

    p_context['connection'].commit()

    return v_data_count, v_failure_count, v_data_size, v_data_size


# ----------------------------------------------------
# Run with array insert
# ----------------------------------------------------
def run_array(p_params, p_context, p_scenario_name, p_run_id):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        g_logger.warning ('Data generator failed with exception: {0}'.format(e))
        raise

    v_data_count = 0
    v_failure_count = 0
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])
    v_data = []
    v_data_size = 0

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)
        v_data_size = v_data_size + (len(v_data_line_json)+len(v_uuid)+len(str(v_timestamp)))

        v_data.append((v_timestamp, v_uuid, p_scenario_name, p_run_id, v_data_line_json))
        v_data_count = v_data_count+1

    p_context['cursor'].setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
    p_context['cursor'].executemany(v_sql, v_data)
    p_context['connection'].commit()
    
    return v_data_count, v_failure_count, v_data_size, v_data_size


# ----------------------------------------------------
# Run with array insert and memoptimize write
# ----------------------------------------------------
def run_fast(p_params, p_context, p_scenario_name, p_run_id):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        g_logger.warning ('Data generator failed with exception: {0}'.format(e))
        raise

    v_data_count = 0
    v_failure_count = 0
    v_sql = 'insert /*+ MEMOPTIMIZE_WRITE */ into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])
    v_data = []
    v_data_size = 0

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)
        v_data_size = v_data_size + (len(v_data_line_json)+len(v_uuid)+len(str(v_timestamp)))

        v_data.append((v_timestamp, v_uuid, p_scenario_name, p_run_id, v_data_line_json))
        v_data_count = v_data_count+1

    p_context['cursor'].setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
    p_context['cursor'].executemany(v_sql, v_data)
    # commit is not used with fast ingest
    #p_context['connection'].commit()

    return v_data_count, v_failure_count, v_data_size, v_data_size


# ----------------------------------------------------
# Put messages to streaming with retries
# - necessary to wrap the standard put_messages() as it does not retry partial failures
# ----------------------------------------------------
def put_messages_with_retry(p_streamclient, p_stream_id, p_messages, p_data, p_max_retries):

    # Put messages to the stream
    v_retry_count = 0
    v_sleep_base_time_sec = 30/1000
    v_sleep_max_time_sec = 3
    v_sleep_total_sec = 0

    try:
        v_put_message_result = p_streamclient.put_messages(stream_id=p_stream_id, put_messages_details=p_messages, retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY)
        v_data_total_count = len(v_put_message_result.data.entries)
        v_data_failure_count = v_put_message_result.data.failures
        v_data_success_count = v_data_total_count - v_data_failure_count
        g_logger.debug('Put messages attempt: retry_count={}, data_total_count={}, data_retry_count={}, data_success_count={}, data_failure_count={}, sleep_time_ms={}'.format(v_retry_count, v_data_total_count, v_data_total_count, v_data_success_count, v_data_failure_count, round(v_sleep_total_sec*1000)))
    except Exception as e:
        g_logger.error ('Put messages failed with exception: {0}'.format(e))
        raise

    # Retry for the failed messages
    while v_data_failure_count > 0 and v_retry_count <= p_max_retries:

        v_exponential_backoff_sleep_base = min(v_sleep_base_time_sec * (2 ** v_retry_count), v_sleep_max_time_sec)
        v_sleep_time = (v_exponential_backoff_sleep_base / 2.0) + random.uniform(0, v_exponential_backoff_sleep_base / 2.0)

        time.sleep(v_sleep_time)

        v_retry_data = []
        v_retry_count = v_retry_count+1
        v_sleep_total_sec = v_sleep_total_sec + v_sleep_time

        for v_entry_index in range(len(v_put_message_result.data.entries)):
            v_entry = v_put_message_result.data.entries[v_entry_index]
            if v_entry.error:
                #g_logger.debug('Put messages error: {}: {}'.format(v_entry.error, v_entry.error_message))
                v_retry_data.append(p_data[v_entry_index])

        v_retry_messages = oci.streaming.models.PutMessagesDetails(messages=v_retry_data)
    
        try:
            v_put_message_result = p_streamclient.put_messages(stream_id=p_stream_id, put_messages_details=v_retry_messages, retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY)
            v_data_retry_count = len(v_put_message_result.data.entries)
            v_data_failure_count = v_put_message_result.data.failures
            v_data_success_count = v_data_success_count + (v_data_retry_count - v_data_failure_count)
            g_logger.debug('Put messages attempt: retry_count={}, data_total_count={}, data_retry_count={}, data_success_count={}, data_failure_count={}, sleep_time_ms={}'.format(v_retry_count, v_data_total_count, v_data_retry_count, v_data_success_count, v_data_failure_count, round(v_sleep_total_sec*1000)))
        except Exception as e:
            g_logger.error ('Put messages failed with exception: {0}'.format(e))
            raise

    return v_retry_count, v_data_total_count, v_data_success_count, v_data_failure_count


# ----------------------------------------------------
# Produce data to streaming
# ----------------------------------------------------
def run_streaming(p_params, p_context, p_scenario_name, p_run_id):

    # Generate data
    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        g_logger.warning ('Data generator failed with exception; {0}'.format(e))
        raise

    v_data_count = 0
    v_failure_count = 0
    v_timestamp = datetime.datetime.today()
    v_data = []
    v_data_size = 0
    v_encoded_size = 0

    # Create array of messages for streaming
    for v_data_line in v_data_array:

        v_value = {
            'uuid': str(uuid.uuid4()),
            'run_id': p_run_id,
            'scenario': p_params['scenario'],
            'timestamp': v_timestamp.isoformat(),
            'data': v_data_line
        }

        v_value_string = json.dumps(v_value)
        v_key_string = v_data_line['journal_external_reference']
        v_value_encoded = b64encode(v_value_string.encode()).decode()
        v_key_encoded = b64encode(v_key_string.encode()).decode()
        v_data_size = v_data_size + (len(v_value_string)+len(v_key_string))
        v_encoded_size = v_encoded_size + (len(v_value_encoded)+len(v_key_encoded))

        v_data.append(oci.streaming.models.PutMessagesDetailsEntry(key=v_key_encoded, value=v_value_encoded))
        v_data_count = v_data_count+1

    v_messages = oci.streaming.models.PutMessagesDetails(messages=v_data)

    # Put messages to the stream
    (v_retry_count, v_data_count, v_success_count, v_failure_count) = put_messages_with_retry(
        p_streamclient=p_context['streamclient'],
        p_stream_id=p_params['topic'],
        p_messages=v_messages,
        p_data=v_data,
        p_max_retries=8
    )

    return v_success_count, v_failure_count, v_data_size, v_encoded_size


# ----------------------------------------------------
# Truncate the target table
# ----------------------------------------------------
def run_truncate(p_params, p_context):

    v_sql = 'truncate table {}'.format(p_params["table"])
    p_context['cursor'].execute(v_sql)

    v_result = {
        'type' : 'init',
        'scenario' : p_params['scenario'],
        'table' : p_params['table'],
        'size' : p_params['size'],
        'message' : 'table truncated'
    }

    return v_result


# ----------------------------------------------------
# Get final statistics
# ----------------------------------------------------
def run_finish(p_params, p_context):

    v_sql = '''
        with
          table_stats as (
            select
              count(distinct run_id) as threads,
              count(*) as inserts,
              nvl(min(ts),systimestamp) as start_ts,
              nvl(max(ts),systimestamp) as end_ts
            from {0}
          ),
          segment_stats as (
            select
              sum(bytes) as bytes,
              sum(blocks) as blocks
            from user_segments
            where segment_name = upper(\'{0}\')
          )
        select
          threads,
          inserts,
          bytes,
          blocks,
          start_ts,
          end_ts
        from table_stats, segment_stats
    '''.format(p_params["table"])

    for row in p_context['cursor'].execute(v_sql):
        v_result = {
           'type' : 'database',
           'scenario' : p_params['scenario'],
           'table' : p_params['table'],
           'size' : p_params['size'],
           'threads' : row[0],
           'start_ts' : row[4].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
           'end_ts' : row[5].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
           'inserts' : row[1],
           'bytes' : row[2],
           'blocks' : row[3]
        }

    return v_result


# ----------------------------------------------------
# CLOSE FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Normal close
# ----------------------------------------------------
def close_oracle(p_params, p_context):

    p_context['cursor'].close()
    return


# ----------------------------------------------------
# Close for fast ingest
# ----------------------------------------------------
def close_oracle_fast(p_params, p_context):

    v_sql = 'begin dbms_memoptimize.write_end; end;'
    p_context['cursor'].execute(v_sql)
    p_context['cursor'].close()
    return


# ----------------------------------------------------
# Close streaming
# ----------------------------------------------------
def close_streaming(p_params, p_context):

    # No action needed
    return


# ----------------------------------------------------
# TASK FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Run single thread
# ----------------------------------------------------
def run_one_thread(p_params, fn_connect, fn_run, fn_close):

    # Initialize
    v_context = dict()
    v_timestamp = dict()
    v_timestamp['start'] = datetime.datetime.today()
    v_run_id = v_timestamp['start'].strftime('%Y%0m%0d_%H%M%S') + '_'+str(os.getpid())

    # Initialize connection
    if fn_connect != None:
        v_context = fn_connect(
            p_params=p_params
        )
    
    # Iterate until the time is exceeded
    v_total_iteration_count = 0
    v_total_data_count = 0
    v_total_failure_count = 0
    v_total_data_size = 0
    v_total_encoded_size = 0
    
    while (datetime.datetime.today()-v_timestamp['start']).total_seconds() <= p_params['duration']:
    
        # Generate and insert data
        (v_data_count, v_failure_count, v_data_size, v_encoded_size) = fn_run(
            p_params=p_params,
            p_context=v_context,
            p_scenario_name='{}-{}-{}'.format(p_params['scenario'], p_params['size'], p_params['threads']),
            p_run_id=v_run_id
        )
    
        # Increment counters
        v_total_iteration_count = v_total_iteration_count+1         
        v_total_data_count = v_total_data_count+v_data_count
        v_total_failure_count = v_total_failure_count+v_failure_count
        v_total_data_size = v_total_data_size+v_data_size
        v_total_encoded_size = v_total_encoded_size+v_encoded_size
    
        # Sleep
        time.sleep(p_params['sleep'])
    
    # Close connection
    if fn_close != None:
        fn_close(
            p_params=p_params,
            p_context=v_context
        )
    
    # Save end timestamp
    v_timestamp['end'] = datetime.datetime.today()
    
    # Create result
    v_result = {
        'type' : 'detail',
        'scenario' : p_params['scenario'],
        'table' : p_params['table'],
        'topic' : p_params['topic'],
        'size' : p_params['size'],
        'threads' : p_params['threads'],
        'thread' : p_params['thread'],
        'minrec' : p_params['minrec'],
        'maxrec' : p_params['maxrec'],
        'iterations' : p_params['iterations'],
        'sleep' : p_params['sleep'],
        'run_id' : v_run_id,
        'max_run_seconds' : p_params['duration'],
        'load_start_datetime' : v_timestamp['start'],
        'load_end_datetime' : v_timestamp['end'],
        'elapsed_sec_total' : (v_timestamp['end']-v_timestamp['start']).total_seconds(),
        'total_iteration_count' : v_total_iteration_count,
        'total_data_count' : v_total_data_count,
        'total_failure_count' : v_total_failure_count,
        'total_data_size' : v_total_data_size,
        'total_encoded_size' : v_total_encoded_size
    }

    return v_result


# ----------------------------------------------------
# Run all threads
# ----------------------------------------------------
def run_all_threads(p_params, fn_connect, fn_run, fn_close):

    # Initialize parameters
    v_timestamp = dict()
    v_timestamp['start'] = datetime.datetime.today()
    v_run_id = v_timestamp['start'].strftime('%Y%0m%0d_%H%M%S')

    v_params_array = []
    for i in range(p_params['threads']):
        v_params = copy.deepcopy(p_params)
        v_params['thread'] = i+1
        v_params_array.append(v_params)

    fn_connect_array = [ fn_connect for i in range(p_params['threads']) ]
    fn_run_array = [ fn_run for i in range(p_params['threads']) ]
    fn_close_array = [ fn_close for i in range(p_params['threads']) ]

    # Run all threads in parallel
    with ProcessPoolExecutor(max_workers=p_params['threads']) as v_executor:
        v_result_set = v_executor.map(run_one_thread, v_params_array, fn_connect_array, fn_run_array, fn_close_array)

    # Consolidate results
    v_result_array = []
    v_result_count = 0

    for v_result in v_result_set:

        v_result_array.append(v_result)

        if (v_result_count <= 0):
            v_result_sum = copy.deepcopy(v_result)
            v_result_sum['type'] = 'sum'
            v_result_sum['thread'] = 0
            v_result_sum['run_id'] = v_result['run_id'][0:15]
        else:

            if v_result['load_start_datetime'] < v_result_sum['load_start_datetime']:
                v_result_sum['load_start_datetime'] = v_result['load_start_datetime']
            
            if v_result['load_end_datetime'] > v_result_sum['load_end_datetime']:
                v_result_sum['load_end_datetime'] = v_result['load_end_datetime']

            v_result_sum['elapsed_sec_total'] = (v_result_sum['load_end_datetime']-v_result_sum['load_start_datetime']).total_seconds()
            
            v_result_sum['total_iteration_count'] = v_result_sum['total_iteration_count'] + v_result['total_iteration_count']
            v_result_sum['total_data_count'] = v_result_sum['total_data_count'] + v_result['total_data_count']
            v_result_sum['total_failure_count'] = v_result_sum['total_failure_count'] + v_result['total_failure_count']
            v_result_sum['total_data_size'] = v_result_sum['total_data_size'] + v_result['total_data_size']
            v_result_sum['total_encoded_size'] = v_result_sum['total_encoded_size'] + v_result['total_encoded_size']

        v_result_count = v_result_count+1

    v_result_array.append(v_result_sum)
    return v_result_array


# ----------------------------------------------------
# Execute init task
# ----------------------------------------------------
def init_task(p_params, fn_connect, fn_run, fn_close):

    # Initialize
    v_context = dict()
    v_timestamp = dict()
    v_timestamp['start'] = datetime.datetime.today()

    # Initialize connection
    if fn_connect != None:
        v_context = fn_connect(
            p_params=p_params
        )
            
    # Run the init task
    if fn_run != None:
        v_result = fn_run(
            p_params=p_params,
            p_context=v_context
        )
    else:
        v_result = None
    
    # Close connection
    if fn_close != None:
        fn_close(
            p_params=p_params,
            p_context=v_context
        )

    return v_result


# ----------------------------------------------------
# Execute finish task
# ----------------------------------------------------
def finish_task(p_params, fn_connect, fn_run, fn_close):

    # Initialize
    v_context = dict()
    v_timestamp = dict()
    v_timestamp['start'] = datetime.datetime.today()

    # Initialize connection
    if fn_connect != None:
        v_context = fn_connect(
            p_params=p_params
        )
            
    # Run the finish task
    if fn_run != None:
        v_result = fn_run(
            p_params=p_params,
            p_context=v_context
        )
    else:
        v_result = None
    
    # Close connection
    if fn_close != None:
        fn_close(
            p_params=p_params,
            p_context=v_context
        )

    return v_result


# ----------------------------------------------------
# MAIN FUNCTION
# ----------------------------------------------------
def main(p_argv):

    # Initialize logging
    initialize_logging(p_argv[0], 'INFO');

    # Get command line parameters
    v_params = get_input_parameters(p_argv)
    g_logger.setLevel(v_params['loglevel'])

    # Initialize functions for scenarios
    if v_params['scenario'] == 'single':
        fn_init_connect,   fn_init_execute,   fn_init_close   = connect_oracle,      run_truncate,  close_oracle
        fn_run_connect,    fn_run_execute,    fn_run_close    = connect_oracle,      run_single,    close_oracle
        fn_finish_connect, fn_finish_execute, fn_finish_close = connect_oracle,      run_finish,    close_oracle
    if v_params['scenario'] == 'batch':
        fn_init_connect,   fn_init_execute,   fn_init_close   = connect_oracle,      run_truncate,  close_oracle
        fn_run_connect,    fn_run_execute,    fn_run_close    = connect_oracle,      run_batch,     close_oracle
        fn_finish_connect, fn_finish_execute, fn_finish_close = connect_oracle,      run_finish,    close_oracle
    if v_params['scenario'] == 'array':
        fn_init_connect,   fn_init_execute,   fn_init_close   = connect_oracle,      run_truncate,  close_oracle
        fn_run_connect,    fn_run_execute,    fn_run_close    = connect_oracle,      run_array,     close_oracle
        fn_finish_connect, fn_finish_execute, fn_finish_close = connect_oracle,      run_finish,    close_oracle
    if v_params['scenario'] == 'fast':
        fn_init_connect,   fn_init_execute,   fn_init_close   = connect_oracle,      run_truncate,  close_oracle
        fn_run_connect,    fn_run_execute,    fn_run_close    = connect_oracle_fast, run_fast,      close_oracle
        fn_finish_connect, fn_finish_execute, fn_finish_close = connect_oracle,      run_finish,    close_oracle
    if v_params['scenario'] == 'stream':
        fn_init_connect,   fn_init_execute,   fn_init_close   = None,                None,          None
        fn_run_connect,    fn_run_execute,    fn_run_close    = connect_streaming,   run_streaming, close_streaming
        fn_finish_connect, fn_finish_execute, fn_finish_close = None,                None,          None

    # Execute init task
    if fn_init_execute != None:

        v_result = init_task(
            p_params=v_params,
            fn_connect=fn_init_connect,
            fn_run=fn_init_execute,
            fn_close=fn_init_close
        )
        if v_result != None:
            print(json.dumps(v_result))

    # Execute run tasks
    if fn_run_execute != None:

        # Run all threads
        v_result_array = run_all_threads(
            p_params=v_params,
            fn_connect=fn_run_connect,
            fn_run=fn_run_execute,
            fn_close=fn_run_close
        )

        # Print results
        for v_result in v_result_array:
            v_result_output = copy.deepcopy(v_result)
            v_result_output['load_start_datetime'] = v_result['load_start_datetime'].strftime('%Y/%0m/%0d %H:%M:%S,%f')
            v_result_output['load_end_datetime'] = v_result['load_end_datetime'].strftime('%Y/%0m/%0d %H:%M:%S,%f')
            print(json.dumps(v_result_output))

    # Execute finish tasks
    if fn_finish_execute != None:
        v_result = finish_task(
            p_params=v_params,
            fn_connect=fn_finish_connect,
            fn_run=fn_finish_execute,
            fn_close=fn_finish_close
        )
        if v_result != None:
            print(json.dumps(v_result))


# ----------------------------------------------------
# Call the main function
# ----------------------------------------------------
main(sys.argv)


# ----------------------------------------------------
# ----------------------------------------------------
# ----------------------------------------------------


