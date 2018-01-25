#!/bin/bash
FILES=./*.txt
echo 'Flight_Num,Scheduled_Dept,Actual_Dept,Dept_Gate,Scheduled_Arrival,Actual_Arrival,Arrival_Gate'> flight-data_truth.csv.tmp
for f in $FILES
do
  #echo "Processing $f file..."
  # take action on each file. $f store current file name
  cat $f |tr ',' ';' | tr '\t' ',' >> flight-data_truth.csv.tmp
done
sed 's/, /,/g' flight-data_truth.csv.tmp > flight-data_truth.csv
rm flight-data_truth.csv.tmp
