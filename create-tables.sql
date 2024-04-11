
drop table gl_stream_single purge
/

drop table gl_stream_batch purge
/

drop table gl_stream_array purge
/

drop table gl_stream_fast purge
/

create table gl_stream_single (
  id varchar2(40) not null,
  run_id varchar2(40) not null,
  scenario varchar2(20) not null,
  ts timestamp not null,
  payload varchar2(4000),
  constraint gl_stream_single_is_json check (payload is json)
)
/

create table gl_stream_batch (
  id varchar2(40) not null,
  run_id varchar2(40) not null,
  scenario varchar2(20) not null,
  ts timestamp not null,
  payload varchar2(4000),
  constraint gl_stream_batch_is_json check (payload is json)
)
/

create table gl_stream_array (
  id varchar2(40) not null,
  run_id varchar2(40) not null,
  scenario varchar2(20) not null,
  ts timestamp not null,
  payload varchar2(4000),
  constraint gl_stream_array_is_json check (payload is json)
)
/

create table gl_stream_fast (
  id varchar2(40) not null,
  run_id varchar2(40) not null,
  scenario varchar2(20) not null,
  ts timestamp not null,
  payload varchar2(4000),
  constraint gl_stream_fast_is_json check (payload is json)
)
nocompress
memoptimize for write
/
