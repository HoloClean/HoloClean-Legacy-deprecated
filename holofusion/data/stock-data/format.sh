head -n 70000 stock-data.csv > stock-data-test.csv
sed -i -e 's/,+/,/g' stock-data-test.csv
sed -i -e 's/,-/,/g' stock-data-test.csv 
sed -i -e 's/%,/,/g' stock-data-test.csv 


