#!/bin/bash
echo "Index,ISBN,Author_list" > book_golden.csv
counter=0
while read p; do
	S="$(echo  "$p" | tr ',' '\@')"
	echo "$counter,$S" | tr '\t' ','  >> book_golden.csv
	
	counter=$((counter+1))
done < book_golden.txt
