 > stock-data-test.csv
mode='read'
readlines=10000
skiplines=35000
counter=0
globalcounter=0
while read -r line
do
	echo $globalcounter
	if [ $mode == 'read' ] 
	then
		echo $line >> stock-data-test.csv	
		counter=$(($counter+1))
		if [ $counter -eq $readlines ] 
		then
			counter=0
			mode='skip'
		fi
	else
		counter=$(($counter+1))
        if [ $counter -eq $skiplines ] 
		then
            counter=0
            mode='read'
        fi
	fi
	globalcounter=$(($globalcounter+1))
done < stock-data.csv

sed -i -e 's/,+/,/g' stock-data-test.csv
sed -i -e 's/,-/,/g' stock-data-test.csv 
sed -i -e 's/%,/,/g' stock-data-test.csv 


