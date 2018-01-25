#!/bin/bash
FILES=./*.txt
filename=flight-data.csv
counter=0
echo 'Source,Flight_Num,Scheduled_Dept,Actual_Dept,Dept_Gate,Scheduled_Arrival,Actual_Arrival,Arrival_Gate'> $filename
for f in $FILES
do
  echo "Processing $f file..."
  # take action on each file. $f store current file name
  > ${f}.tmp
  cat $f | tr ',' '-' | tr '\t' ',' >> ${f}.tmp
  date=${f:2:11}
  sed -i "s|,|,$date|1" ${f}.tmp
  cat ${f}.tmp >> $filename
  rm ${f}.tmp
done
sed -i 's/,[^,]*//7' ${filename}
sed -i 's/,[^,]*//4' ${filename}
perl -pi -e 's/,[^,]*(?=([^,][0-9]:[0-9]{2}))/,/g' $filename
perl -pi -e 's/(?<=[0-9]:[0-9]{2})[^,\n]*//g' $filename
perl -pi -e 's/[\ 0](?=[0-9]:[0-9]{2})//g' $filename
