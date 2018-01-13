#!/bin/bash
echo "Source,ISBN,Title,Author_list" > book.csv

while read p; do
	S="$(echo  "$p" | tr ',' '\@')"
	echo "$S" | tr '\t' ',' >> book.csv
done < book.txt
