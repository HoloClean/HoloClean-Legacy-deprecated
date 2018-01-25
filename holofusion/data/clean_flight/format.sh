mode='read'
readlines=10
skiplines=65
counter=0
globalcounter=0
filename=flight-data
> ${filename}-test.csv
while read -r line
do
	echo $globalcounter
	if [ $mode == 'read' ] 
	then
		echo $line >> ${filename}-test.csv	
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
done < ${filename}.csv

sed -i -e 's/,+/,/g' ${filename}-test.csv
sed -i -e 's/,-/,/g' ${filename}-test.csv 
sed -i -e 's/%,/,/g' ${filename}-test.csv 


