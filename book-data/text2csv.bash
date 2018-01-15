#!/bin/bash
echo "Index,Source,ISBN,Title,Author_list" > book.csv
counter=0
while read p; do
#	odd=$((counter % 2))
#	if [ $odd -eq 0 ]
#	then
		S="$(echo  "$p" | tr ',' '\@'| tr A-Z a-z)"
		S="$(echo "$((counter/2)),$S" | tr '\t' ',')"
		S=${S//' ;'/';'}
		S=${S//'  '/' '}
		S=${S//';  '/'; '}
		echo "$S" | tr ' ' '_'   >> book.csv
#	fi
	counter=$((counter+1))
done < book.txt
