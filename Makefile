# Requires that you install docker 'sudo apt-get install docker.io'
build-holo-docker:
	docker build -t holo/clean:0.1 .
run-holo-docker:
	docker run --net host -it holo/clean:0.1 /bin/bash
start-mysql-server:
	# Connect using command:
	# mysql -u holocleanUser -P 4444 -h 127.0.0.1 -p
	# Check the logs using: docker logs holoclean-mysql
	docker run --name=holoclean-mysql -d -p 4444:3306 -e MYSQL_ROOT_PASSWORD='root' -e MYSQL_DATABASE='holo' -e MYSQL_USER='holocleanUser' -e MYSQL_PASSWORD='abcd1234' mysql/mysql-server:5.7
stop-mysql-server:
	docker rm -f holoclean-mysql