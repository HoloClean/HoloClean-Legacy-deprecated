#!/bin/bash
echo "Index,ISBN,Author_list" > book_golden.csv
counter=0
while read p; do
	odd=$((counter % 2))
	if [ $odd -eq 0 ]
	then
		S="$(echo  "$p" | tr ',' '\@')"
		S="$(echo "$((counter/2)),$S" | tr '\t' ',')"
		S=${S//';  '/','}
		S=${S//' ,'/','}
		S=${S//' ;'/';'}
		echo "$S" | tr ' ' '_'  >> book_golden.csv
	fi
	counter=$((counter+1))
done < book_golden.txt
