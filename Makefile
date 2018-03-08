# Requires that you install docker 'sudo apt-get install docker.io'
build-holo-docker:
	docker build -t holoclean/holoclean:0.1 .
run-holo-docker:
	docker run -p 8888:8888 -p 5555:3306  holoclean/holoclean:0.1 
