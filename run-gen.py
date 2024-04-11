import string
import datetime
import random
import json
import sys
import logging
import codecs
import uuid
import os
import getopt

from dateutil.relativedelta import relativedelta

import oracledb


# ----------------------------------------------------
# SETUP FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Get input parameters
# ----------------------------------------------------
def get_input_parameters(p_argv):

   v_usage = '{} -a <action> -s <scenario> -z <size> -t <threads> -r <thread> -d <duration> -x <min records> -y <max records> -i <iterations> -b <table> -u <dbuser> -p <dbpwd> -c <dbconnect>'.format(p_argv[0])

   v_params = {
      'action':      None,
      'scenario':    None,
      'size':        None,
      'threads':     None,
      'thread':      None,
      'duration':    None,
      'minrec':      60,
      'maxrec':      100,
      'iterations':  1,
      'table':       None,
      'dbuser':      None,
      'dbpwd':       None,
      'dbconnect':   None
   } 
     
   v_help = '''
   Options:
   -h, --help             Print help
   -a, --action           Action to perform (init, run, finish) [mandatory]
   -s, --scenario         Scenario (single, batch, array, fast) [mandatory]
   -z, --size             Size of database instance [mandatory if action=run]
   -t, --threads          Number of threads [mandatory if action=run]
   -r, --thread           Current thread [mandatory if action=run]
   -d, --duration         Duration in seconds [mandatory if action=run]
   -x, --minrec           Minimum number of records in iteration [{0}]
   -y, --maxrec           Maximum number of records in iteration [{1}]
   -i, --iterations       Number of iterations before write to database [{2}]
   -b, --table            Name of target table [mandatory]
   -u, --dbuser           Database user [mandatory]
   -p, --dbpwd            Database user password [mandatory]
   -c, --dbconnect        Database connect string [mandatory]
   '''.format(v_params['minrec'], v_params['maxrec'], v_params['iterations'])

   try:
      (v_opts, v_args) = getopt.getopt(p_argv[1:],"ha:s:z:t:r:d:x:y:i:b:u:p:c:",['help','action=','scenario=','size=','threads=','thread=','duration=','minrec=','maxrec=','iterations=','table=','dbuser=','dbpwd=','dbconnect='])
   except getopt.GetoptError:
      g_logger.error ('Unknown parameter or parameter with missing value')
      print (v_usage)
      sys.exit(2)
   for v_opt, v_arg in v_opts:
      if v_opt in ('-h', '--help'):
         print (v_usage)
         print (v_help)
         sys.exit()
      elif v_opt in ('-a', '--action'):
         v_params['action'] = v_arg
      elif v_opt in ('-s', '--scenario'):
         v_params['scenario'] = v_arg
      elif v_opt in ('-z', '--size'):
         v_params['size'] = v_arg
      elif v_opt in ('-t', '--threads'):
         v_params['threads'] = int(v_arg)
      elif v_opt in ('-r', '--thread'):
         v_params['thread'] = int(v_arg)
      elif v_opt in ('-d', '--duration'):
         v_params['duration'] = int(v_arg)
      elif v_opt in ('-d', '--duration'):
         v_params['duration'] = int(v_arg)
      elif v_opt in ('-x', '--minrec'):
         v_params['minrec'] = int(v_arg)
      elif v_opt in ('-y', '--maxrec'):
         v_params['maxrec'] = int(v_arg)
      elif v_opt in ('-i', '--iterations'):
         v_params['iterations'] = int(v_arg)
      elif v_opt in ('-b', '--table'):
         v_params['table'] = v_arg
      elif v_opt in ('-u', '--dbuser'):
         v_params['dbuser'] = v_arg
      elif v_opt in ('-p', '--dbpwd'):
         v_params['dbpwd'] = v_arg
      elif v_opt in ('-c', '--dbconnect'):
         v_params['dbconnect'] = v_arg

   if v_params['action'] == None:
      g_logger.error ('Missing value for parameter "action"')
      print (v_usage)
      sys.exit(2)
   elif v_params['action'] not in ('init', 'run', 'finish'):
      g_logger.error ('Parameter "action" must have value "init", "run", or "finish"')
      print (v_usage)
      sys.exit(2)
   if v_params['scenario'] == None:
      g_logger.error ('Missing value for parameter "scenario"')
      print (v_usage)
      sys.exit(2)
   elif v_params['scenario'] not in ('single', 'batch', 'array', 'fast'):
      g_logger.error ('Parameter "scenario" must have value "single", "batch", "array", or "fast"')
      print (v_usage)
      sys.exit(2)
   if v_params['size'] == None and v_params['action'] == 'run':
      g_logger.error ('Missing value for parameter "size"')
      print (v_usage)
      sys.exit(2)
   elif v_params['threads'] == None and v_params['action'] == 'run':
      g_logger.error ('Missing value for parameter "threads"')
      print (v_usage)
      sys.exit(2)
   elif v_params['thread'] == None and v_params['action'] == 'run':
      g_logger.error ('Missing value for parameter "thread"')
      print (v_usage)
      sys.exit(2)
   elif v_params['duration'] == None and v_params['action'] == 'run':
      g_logger.error ('Missing value for parameter "duration"')
      print (v_usage)
      sys.exit(2)
   elif v_params['minrec'] == None and v_params['minrec'] == 'run':
      g_logger.error ('Missing value for parameter "minrec"')
      print (v_usage)
      sys.exit(2)
   elif v_params['maxrec'] == None and v_params['maxrec'] == 'run':
      g_logger.error ('Missing value for parameter "maxrec"')
      print (v_usage)
      sys.exit(2)
   elif v_params['iterations'] == None and v_params['iterations'] == 'run':
      g_logger.error ('Missing value for parameter "iterations"')
      print (v_usage)
      sys.exit(2)
   elif v_params['table'] == None:
      g_logger.error ('Missing value for parameter "table"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbuser'] == None:
      g_logger.error ('Missing value for parameter "dbuser"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbpwd'] == None:
      g_logger.error ('Missing value for parameter "dbpwd"')
      print (v_usage)
      sys.exit(2)
   elif v_params['dbconnect'] == None:
      g_logger.error ('Missing value for parameter "dbconnect"')
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
# Normal connect
# ----------------------------------------------------
def connect_oracle(p_params):

    oracledb.init_oracle_client()

    try:
        v_connection = oracledb.connect(
            user=p_params['dbuser'],
            password=p_params['dbpwd'],
            dsn=p_params['dbconnect']
        )
        v_cursor = v_connection.cursor()

    except Exception as e:
        logging.warning ('Cannot connect to the database {0}'.format(e))
        raise

    return (v_connection, v_cursor)


# ----------------------------------------------------
# Connect for fast ingest
# ----------------------------------------------------
def connect_oracle_fast(p_params):

    oracledb.init_oracle_client()

    try:
        v_connection = oracledb.connect(
            user=p_params['dbuser'],
            password=p_params['dbpwd'],
            dsn=p_params['dbconnect']
        )
        v_cursor = v_connection.cursor()
        v_cursor.execute('alter session set optimizer_ignore_hints = false')

    except Exception as e:
        logging.warning ('Cannot connect to the database {0}'.format(e))
        raise

    return (v_connection, v_cursor)


# ----------------------------------------------------
# RUN FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Run with commit after every row
# ----------------------------------------------------
def run_single(p_params, p_scenario_name, p_run_id, p_connection, p_cursor):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        logging.warning ('Data generator failed with exception {0}'.format(e))
        raise

    v_data_count = 0
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)

        p_cursor.setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
        p_cursor.execute(v_sql, ts=v_timestamp, id=v_uuid, scenario=p_scenario_name, run_id=p_run_id, payload=v_data_line_json)
        p_connection.commit()

        v_data_count = v_data_count+1

    return v_data_count


# ----------------------------------------------------
# Run with commit after the batch
# ----------------------------------------------------
def run_batch(p_params, p_scenario_name, p_run_id, p_connection, p_cursor):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        logging.warning ('Data generator failed with exception {0}'.format(e))
        raise

    v_data_count = 0
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)

        p_cursor.setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
        p_cursor.execute(v_sql, ts=v_timestamp, id=v_uuid, scenario=p_scenario_name, run_id=p_run_id, payload=v_data_line_json)

        v_data_count = v_data_count+1

    p_connection.commit()
    return v_data_count


# ----------------------------------------------------
# Run with array insert
# ----------------------------------------------------
def run_array(p_params, p_scenario_name, p_run_id, p_connection, p_cursor):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        logging.warning ('Data generator failed with exception {0}'.format(e))
        raise

    v_data_count = 0
    v_data = []
    v_sql = 'insert into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)

        v_data.append((v_timestamp, v_uuid, p_scenario_name, p_run_id, v_data_line_json))
        v_data_count = v_data_count+1

    p_cursor.setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
    p_cursor.executemany(v_sql, v_data)
    p_connection.commit()
    
    return v_data_count


# ----------------------------------------------------
# Run with array insert and memoptimize write
# ----------------------------------------------------
def run_fast(p_params, p_scenario_name, p_run_id, p_connection, p_cursor):

    try:
        v_data_array = get_journals(p_params["iterations"], p_params["minrec"], p_params["maxrec"])
    except Exception as e:
        logging.warning ('Data generator failed with exception {0}'.format(e))
        raise

    v_data_count = 0
    v_data = []
    v_sql = 'insert /*+ MEMOPTIMIZE_WRITE */ into {} (ts, id, scenario, run_id, payload) values (:ts, :id, :scenario, :run_id, :payload)'.format(p_params["table"])

    for v_data_line in v_data_array:

        v_timestamp = datetime.datetime.today()
        v_uuid = str(uuid.uuid4())
        v_data_line_json = json.dumps(v_data_line)

        v_data.append((v_timestamp, v_uuid, p_scenario_name, p_run_id, v_data_line_json))
        v_data_count = v_data_count+1

    p_cursor.setinputsizes = (oracledb.DB_TYPE_TIMESTAMP)
    p_cursor.executemany(v_sql, v_data)
    # commit is not used with fast ingest
    #p_connection.commit()

    return v_data_count


# ----------------------------------------------------
# Truncate the target table
# ----------------------------------------------------
def run_truncate(p_params, p_connection, p_cursor):

    v_sql = 'truncate table {}'.format(p_params["table"])
    p_cursor.execute(v_sql)

    v_result = {
       'action' : p_params['action'],
       'scenario' : p_params['scenario'],
       'size' : p_params['size'],
       'table' : p_params['table'],
       'message' : 'table truncated'
    }

    return v_result


# ----------------------------------------------------
# Get final statistics
# ----------------------------------------------------
def run_finish(p_params, p_connection, p_cursor):

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

    for row in p_cursor.execute(v_sql):
        v_result = {
           'action' : p_params['action'],
           'scenario' : p_params['scenario'],
           'size' : p_params['size'],
           'table' : p_params['table'],
           'threads' : row[0],
           'inserts' : row[1],
           'bytes' : row[2],
           'blocks' : row[3],
           'start_ts' : row[4].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
           'end_ts' : row[5].strftime('%Y/%0m/%0d %H:%M:%S,%f')
        }

    return v_result


# ----------------------------------------------------
# CLOSE FUNCTIONS
# ----------------------------------------------------

# ----------------------------------------------------
# Normal close
# ----------------------------------------------------
def close_oracle(p_params, p_connection, p_cursor):

    p_cursor.close()
    return


# ----------------------------------------------------
# Close for fast ingest
# ----------------------------------------------------
def close_oracle_fast(p_params, p_connection, p_cursor):

    v_sql = 'begin dbms_memoptimize.write_end; end;'
    p_cursor.execute(v_sql)
    p_cursor.close()
    return


# ----------------------------------------------------
# MAIN FUNCTION
# ----------------------------------------------------
def main(p_argv):

    # Save start timestamp
    v_timestamp = dict()
    v_timestamp['start'] = datetime.datetime.today()

    # Initialize logging
    initialize_logging(p_argv[0], 'DEBUG');

    # Initialize global variables
    v_run_id = v_timestamp['start'].strftime('%Y%0m%0d_%H%M%S') + '_'+str(os.getpid())

    # Get command line parameters
    v_params = get_input_parameters(p_argv)

    # Initialize functions for scenarios
    if v_params['action'] == 'run' and v_params['scenario'] == 'single':
        fn_connect = connect_oracle
        fn_run = run_single
        fn_close = close_oracle
    elif v_params['action'] == 'run' and v_params['scenario'] == 'batch':
        fn_connect = connect_oracle
        fn_run = run_batch
        fn_close = close_oracle
    elif v_params['action'] == 'run' and v_params['scenario'] == 'array':
        fn_connect = connect_oracle
        fn_run = run_array
        fn_close = close_oracle
    elif v_params['action'] == 'run' and v_params['scenario'] == 'fast':
        fn_connect = connect_oracle_fast
        fn_run = run_fast
        fn_close = close_oracle_fast
    elif v_params['action'] == 'init':
        fn_connect = connect_oracle
        fn_run = run_truncate
        fn_close = close_oracle
    elif v_params['action'] == 'finish':
        fn_connect = connect_oracle
        fn_run = run_finish
        fn_close = close_oracle

    # Execute init and finish tasks
    if v_params['action'] in ('init', 'finish'):

        # Initialize Oracle connection
        (v_connection, v_cursor) = fn_connect(p_params=v_params)

        # Rn the action
        v_result = fn_run(
            p_params=v_params,
            p_connection=v_connection,
            p_cursor=v_cursor
        )
        
        # Close Oracle connection
        fn_close(
            p_params=v_params,
            p_connection=v_connection,
            p_cursor=v_cursor
        )

    # Execute run tasks
    elif v_params['action'] == 'run':

        # Initialize Oracle connection
        (v_connection, v_cursor) = fn_connect(p_params=v_params)
    
        # Iterate until the time is exceeded
        v_total_iteration_count = 0
        v_total_data_count = 0
    
        while (datetime.datetime.today()-v_timestamp['start']).total_seconds() <= v_params['duration']:
    
            # Generate and insert data
            v_data_count = fn_run(
                p_params=v_params,
                p_scenario_name='{}-{}-{}'.format(v_params['scenario'], v_params['size'], v_params['threads']),
                p_run_id=v_run_id,
                p_connection=v_connection,
                p_cursor=v_cursor
            )
    
            # Increment counters
            v_total_iteration_count = v_total_iteration_count+1         
            v_total_data_count = v_total_data_count+v_data_count
    
        # Close Oracle connection
        fn_close(
            p_params=v_params,
            p_connection=v_connection,
            p_cursor=v_cursor
        )
    
        # Save end timestamp
        v_timestamp['end'] = datetime.datetime.today()
    
        # Create result
        v_result = {
            'action' : v_params['action'],
            'scenario' : v_params['scenario'],
            'size' : v_params['size'],
            'table' : v_params['table'],
            'threads' : v_params['threads'],
            'current_thread' : v_params['thread'],
            'run_id' : v_run_id,
            'max_run_seconds' : v_params['duration'],
            'load_start_datetime' : v_timestamp['start'].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
            'load_end_datetime' : v_timestamp['end'].strftime('%Y/%0m/%0d %H:%M:%S,%f'),
            'elapsed_sec_total' : (v_timestamp['end']-v_timestamp['start']).total_seconds(),
            'total_iteration_count' : v_total_iteration_count,
            'total_data_count' : v_total_data_count
        }

    print (json.dumps(v_result))


# ----------------------------------------------------
# Call the main function
# ----------------------------------------------------
main(sys.argv)


# ----------------------------------------------------
# ----------------------------------------------------
# ----------------------------------------------------


