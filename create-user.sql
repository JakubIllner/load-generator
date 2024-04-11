
create user loadgen
identified by replaceWithYourPassword
quota unlimited on data
/

grant create session,
create table,
create view,
create procedure,
create sequence
to loadgen
/

grant execute on dbms_cloud
to loadgen
/

