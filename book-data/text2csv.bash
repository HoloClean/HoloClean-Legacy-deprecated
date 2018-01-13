#!/bin/bash
echo "Index,Source,ISBN,Title,Author_list" > book.csv
counter=0
while read p; do
	S="$(echo  "$p" | tr ',' '\@')"
	echo "$counter,$S" | tr '\t' ',' | tr ' ' '_' >> book.csv
	
	counter=$((counter+1))
done < book.txt
