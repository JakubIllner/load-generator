# __load-generator__

## Purpose

The main purpose of the `load-generator` is to generate and test parallel data ingestion
workload into Oracle Database and other Oracle Cloud services such as Streaming.

Similar workload might be coming from IoT use cases, where the requirement is to ingest
large data volumes with minimal latency, so that data is available for querying and
analysis without the need to batch it outside the database.


## Scenarios

The `load-generator` supports the following scenarios:

* __single__ - Records are inserted into an Oracle table one-by-one and immediately committed.
* __batch__ - Records are inserted into an Oracle table one-by-one and committed after a batch of records is inserted.
* __array__ - Records are inserted into an Oracle table in arrays and committed after every array insert.
* __fast__ - Records are inserted into an Oracle table in arrays using the Fast Ingest available from Oracle Database 19c.
* __stream__ - Records are published to a stream in the OCI Streaming service using OCI Streaming API.


## Data Model

### Database

For Oracle Database, the `load-generator` uses a simple data model with JSON payload and
several metadata columns.

```
create table gl_stream (
  id varchar2(40) not null,
  run_id varchar2(40) not null,
  scenario varchar2(20) not null,
  ts timestamp not null,
  payload varchar2(4000),
  constraint gl_stream_is_json check (payload is json)
);
```

### Streaming

For OCI Streaming, the `load-generator` generates JSON messages with the following
data structure.

```
{
  "uuid": "<message unique identifier",
  "run_id": "<load run identifier>",
  "scenario": "<scenario>",
  "timestamp": "<timestamp>",
  "data": { <payload> }
}
```

The key is taken from `journal_external_reference` field of the payload.


### Payload

Payload is a JSON document corresponding to General Ledger journal lines. Data is randomly generated.

```
{
  "journal_header_source_code": "MBT",
  "journal_external_reference": "7WND9GIBV00H2YP6B1WV",
  "journal_header_description": "opxhvlbkbbzkqczocnndfqlqaewnqzmifiblmhaqm",
  "period_code": "202406",
  "period_date": "2024-06-30T00:00:00",
  "currency_code": "USD",
  "journal_category_code": "UXB",
  "journal_posted_date": "2024-06-12T00:00:00",
  "journal_created_date": "2024-06-12T00:00:00",
  "journal_created_timestamp": "2024-06-12T08:17:21.898729",
  "journal_actual_flag": "Y",
  "journal_status": "LAZ",
  "journal_header_name": "pnajetcjupxyjwtdpjbhimkafmwjlkiktpuctxl",
  "reversal_flag": "N",
  "reversal_journal_header_source_code": "",
  "journal_line_number": 31,
  "account_code": "A246",
  "organization_code": "R033",
  "project_code": "P783",
  "journal_line_type": "CR",
  "entered_debit_amount": 0,
  "entered_credit_amount": 8043.517930780035,
  "accounted_debit_amount": 0,
  "accounted_credit_amount": 8043.517930780035,
  "journal_line_description": "cakiamqfbtzefcdibifmpqnxhrnelbdsiylbmpymoujo"
}
```


## Generator

The `load-generator` is a single Python3 program `run-gen.py` that generates data and
inserts them into the Oracle Database table or OCI Streaming stream according to the
selected scenario. It finishes after required time.

The actions performed by the program `run-gen.py` are:

![Run](images/load-generator-run.png)

* __Initialize__ - Initialize the environment for the run.
* __Run__ - Run the load asynchronously, by multiple parallel load processes.
* __Finish__ - Perform final tasks after the run, such as publishing load statistics.

Note that procedures `fn_init*()`, `fn_run*()`, and `fn_finish*()` differ according to the
scenario. For example, scenarios loading into Oracle Database use `python-oracledb` to
interact with Oracle Database, while the scenario publishing messages to Streaming uses
`oci` - Oracle Cloud Infrastructure Python SDK.

The `run-gen.py` uses `ProcessPoolExecutor` from `concurrent.futures` to run multiple
load processes asynchronously, in parallel. It produces load statistics for every load
process as well as summary statistics for the run.


## Parameters

The program `run-gen.py` is parameterized by the following parameters:

```
$ python run-gen.py -h
run-gen.py -s <scenario> -z <size> -t <threads> -d <duration> -x <min records> -y <max records> -i <iterations> -e <sleep> -b <table> -u <dbuser> -p <dbpwd> -c <dbconnect> -o <topic ocid>

Options:
-h, --help             Print help
-s, --scenario         Scenario (single, batch, array, fast, stream) [mandatory]
-z, --size             Size of database or streaming instance [mandatory]
-t, --threads          Number of threads [mandatory]
-d, --duration         Duration in seconds [mandatory]
-x, --minrec           Minimum number of records in iteration [60]
-y, --maxrec           Maximum number of records in iteration [100]
-i, --iterations       Number of iterations before write to database [1]
-e, --sleep            Sleep time in seconds between iterations [0]
-b, --table            Name of target table [mandatory if scenario=single|batch|array|fast]
-u, --dbuser           Database user [mandatory if scenario=single|batch|array|fast]
-p, --dbpwd            Database user password [mandatory if scenario=single|batch|array|fast]
-c, --dbconnect        Database connect string [mandatory if scenario=single|batch|array|fast]
-o, --topic            Streaming topic OCID [mandatory if scenario=stream]
    --loglevel         Log level [DEBUG, INFO, WARNING, ERROR, CRITICAL], default is INFO
```


## Output

You may use JSON output generated by the `run-gen.py` to evaluate performance of the load.
Below is an example of the output for the `array` scenario with 4 threads, running for 60
seconds.

```
$ python run-gen.py -s array -z 4thread -t 4 -d 60 -e 0 --table GL_STREAM_ARRAY --dbuser <DBUSER> --dbpwd <DBPWD> --dbconnect <DBCONNECT> --loglevel INFO
{"type": "init", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "message": "table truncated"}
{"type": "detail", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "thread": 1, "minrec": 60, "maxrec": 100, "iterations": 1, "sleep": 0, "run_id": "20240612_101432_1582792", "max_run_seconds": 60, "load_start_datetime": "2024/06/12 10:14:32,998114", "load_end_datetime": "2024/06/12 10:15:33,005819", "elapsed_sec_total": 60.007705, "total_iteration_count": 4210, "total_data_count": 338422, "total_failure_count": 0, "total_data_size": 347501256, "total_encoded_size": 347501256}
{"type": "detail", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "thread": 2, "minrec": 60, "maxrec": 100, "iterations": 1, "sleep": 0, "run_id": "20240612_101432_1582793", "max_run_seconds": 60, "load_start_datetime": "2024/06/12 10:14:32,998673", "load_end_datetime": "2024/06/12 10:15:33,009669", "elapsed_sec_total": 60.010996, "total_iteration_count": 4306, "total_data_count": 345656, "total_failure_count": 0, "total_data_size": 355117133, "total_encoded_size": 355117133}
{"type": "detail", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "thread": 3, "minrec": 60, "maxrec": 100, "iterations": 1, "sleep": 0, "run_id": "20240612_101432_1582794", "max_run_seconds": 60, "load_start_datetime": "2024/06/12 10:14:32,998757", "load_end_datetime": "2024/06/12 10:15:33,010378", "elapsed_sec_total": 60.011621, "total_iteration_count": 4269, "total_data_count": 340866, "total_failure_count": 0, "total_data_size": 350205075, "total_encoded_size": 350205075}
{"type": "detail", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "thread": 4, "minrec": 60, "maxrec": 100, "iterations": 1, "sleep": 0, "run_id": "20240612_101432_1582795", "max_run_seconds": 60, "load_start_datetime": "2024/06/12 10:14:32,999703", "load_end_datetime": "2024/06/12 10:15:33,005484", "elapsed_sec_total": 60.005781, "total_iteration_count": 4306, "total_data_count": 344807, "total_failure_count": 0, "total_data_size": 354208156, "total_encoded_size": 354208156}
{"type": "sum", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "thread": 0, "minrec": 60, "maxrec": 100, "iterations": 1, "sleep": 0, "run_id": "20240612_101432", "max_run_seconds": 60, "load_start_datetime": "2024/06/12 10:14:32,998114", "load_end_datetime": "2024/06/12 10:15:33,010378", "elapsed_sec_total": 60.012264, "total_iteration_count": 17091, "total_data_count": 1369751, "total_failure_count": 0, "total_data_size": 1407031620, "total_encoded_size": 1407031620}
{"type": "database", "scenario": "array", "table": "GL_STREAM_ARRAY", "size": "4thread", "threads": 4, "start_ts": "2024/06/12 10:14:33,000000", "end_ts": "2024/06/12 10:15:33,000000", "inserts": 1369751, "bytes": 285212672, "blocks": 34816}
```


## Prerequisites

Before running the `load-generator`, ensure the following prerequisites are met:

* Oracle Database 19c+ instance. Note I tested the program with Autonomous Data Warehouse Serverless (ADW).
* OCI Streaming stream instance. Note I tested the program with the public connectivity to OCI Streaming.
* Compute instance with Python3 (tested with 3.9), Oracle Database client 19c+, `python-oracledb`, and `oci` packages.
* Network connectivity from the Compute instance to the Oracle Database for SQL Net (typically TCP/1521-1522).
* Network connectivity from the Compute instance to the Streaming API (public or private, over TCP/443).
* Database wallet for mTLS connection and configured SQL Net on the Compute instance.
* Database schema with target tables. Refer to `create-user.sql` file.
* Target table(s) deployed in the schema. Refer to `create-tables.sql` file.
* Configured `~/.oci/config` with API Key to connect to OCI API with Python SDK. Note the `run-gen.py` currently does not support instance principal authentication.


## Considerations

* The Compute instance with the `load-generator` program must be sized accordingly.
Particularly for the Fast Ingest (scenario `fast`) and higher levels of parallelism, the
client Compute instance requires lot of OCPUs and sufficient network bandwidth.

* You can easily modify the procedures `get_journals()` and `get_journal_lines()` to produce
different payloads. Rest of the program does not care about the structure of the payload.

* You can easily modify the target table(s) according to your needs. For example, you can
add indexes to measure the impact of indexes on the performance. Also, you can change the
payload data type from `VARCHAR2(4000)` to `BLOB` for larger payloads, although this will
have detrimental impact on the load performance.




