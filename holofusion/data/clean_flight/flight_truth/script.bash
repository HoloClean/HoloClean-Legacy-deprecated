#!/bin/bash
FILES=./*.txt
filename=flight-data_truth.csv
echo 'Flight_Num,Scheduled_Dept,Actual_Dept,Dept_Gate,Scheduled_Arrival,Actual_Arrival,Arrival_Gate'> $filename.tmp
for f in $FILES
do
  #echo "Processing $f file..."
  # take action on each file. $f store current file name
  cat $f |tr ',' ';' | tr '\t' ',' >> $filename.tmp
done
sed 's/, /,/g' $filename.tmp > $filename
rm $filename.tmp
sed -i 's/,[^,]*//6' ${filename}
sed -i 's/,[^,]*//3' ${filename}

perl -pi -e 's/,[^,]*(?=([^,][0-9]:[0-9]{2}))/,/g' $filename
perl -pi -e 's/(?<=[0-9]:[0-9]{2})[^,\n]*//g' $filename
perl -pi -e 's/0(?=[0-9]:[0-9]{2})//g' $filename

