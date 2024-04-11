#!/usr/bin/bash

scenario=array
size=4ecpu
table=gl_stream_array
duration=60
threads=4
dbuser=loadgen
dbpwd=replaceWithYourPassword
dbconnect=myadw_low

./run-gen.sh ${scenario} ${size} ${table} ${duration} ${threads} ${dbuser} ${dbpwd} ${dbconnect}

