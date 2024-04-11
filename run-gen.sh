#!/usr/bin/bash

scenario=${1}
size=${2}
table=${3}
duration=${4}
threads=${5}
dbuser=${6}
dbpwd=${7}
dbconnect=${8}

minrec=60
maxrec=100
iterations=1
times=`date +%Y%m%d%H%M%S`

outfile=out/run-init-$times}-${scenario}-${size}-${threads}-out.log
errfile=err/run-init-$times}-${scenario}-${size}-${threads}-err.log
python3 run-gen.py -a init -s ${scenario} -z ${size} -b ${table} -u ${dbuser} -p ${dbpwd} -c ${dbconnect} > ${outfile} 2> ${errfile}

for (( i=1; i<=${threads}; ++i)); do
    outfile=out/run-gen-${times}-${scenario}-${size}-${threads}-${i}-out.log
    errfile=err/run-gen-${times}-${scenario}-${size}-${threads}-${i}-err.log
    python3 run-gen.py -a run -s ${scenario} -z ${size} -b ${table} -t ${threads} -r ${i} -d ${duration} -x ${minrec} -y ${maxrec} -i ${iterations} -u ${dbuser} -p ${dbpwd} -c ${dbconnect} > ${outfile} 2> ${errfile} &
done

wait

outfile=out/run-finish-${times}-${scenario}-${size}-${threads}-out.log
errfile=err/run-finish-${times}-${scenario}-${size}-${threads}-err.log
python3 run-gen.py -a finish -s ${scenario} -z ${size} -b ${table} -u ${dbuser} -p ${dbpwd} -c ${dbconnect} > ${outfile} 2> ${errfile}


