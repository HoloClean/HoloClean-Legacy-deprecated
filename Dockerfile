FROM ubuntu:16.04
MAINTAINER HoloClean "holocleanteam@gmail.com"

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y python2.7 python-pip
RUN pip install http://download.pytorch.org/whl/cpu/torch-0.3.1-cp27-cp27mu-linux_x86_64.whl && \
    pip install torchvision 

# Copy all the required folders into the docker image
COPY python-package-requirement.txt /holoclean/
COPY holoclean /holoclean/holoclean/
COPY test /holoclean/test/
COPY datasets 	/holoclean/datasets/
COPY tutorial   /holoclean/tutorial/
COPY LICENSE /holoclean/

WORKDIR /holoclean
# Install VIM and git, not really needed
RUN apt-get install -y vim
RUN apt-get install -y git

# Install required python packages
RUN apt-get install -y libmysqlclient-dev
RUN pip install -r python-package-requirement.txt

# Install Java to run Spark.
RUN  apt-get update && \
  apt-get -y install software-properties-common && \
  add-apt-repository -y ppa:webupd8team/java && \
  echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections && \
  apt-get update && \
  apt-get -y install oracle-java8-installer

# Install MySQL
RUN echo "deb http://cn.archive.ubuntu.com/ubuntu/ xenial main restricted universe multiverse" >> /etc/apt/sources.list
RUN apt-get -y install mysql-server-5.7 && \
	mkdir -p /var/lib/mysql && \
	mkdir -p /var/run/mysqld && \
	mkdir -p /var/log/mysql && \
	chown -R mysql:mysql /var/lib/mysql && \
	chown -R mysql:mysql /var/run/mysqld && \
	chown -R mysql:mysql /var/log/mysql
# MySQL UTF-8 and bind-address
RUN sed -i -e "$ a [client]\n\n[mysql]\n\n[mysqld]"  /etc/mysql/my.cnf && \
	sed -i -e "s/\(\[client\]\)/\1\ndefault-character-set = utf8/g" /etc/mysql/my.cnf && \
	sed -i -e "s/\(\[mysql\]\)/\1\ndefault-character-set = utf8/g" /etc/mysql/my.cnf && \
	sed -i -e "s/\(\[mysqld\]\)/\1\ninit_connect='SET NAMES utf8'\ncharacter-set-server = utf8\ncollation-server=utf8_unicode_ci\nbind-address = 0.0.0.0/g" /etc/mysql/my.cnf
# Mount MySQL data directory
VOLUME /var/lib/mysql

# Start MySQL and set up HoloClean database and user
COPY setup_mysql_user.sh /holoclean/
RUN chmod +x /holoclean/setup_mysql_user.sh
ENTRYPOINT ["/holoclean/setup_mysql_user.sh"]

# Set Environment Variables for HoloClean
#RUN /bin/bash -c "source set_env.sh"
ENV HOLOCLEANHOME /holoclean
ENV PYTHONPATH $PYTHONPATH:$HOLOCLEANHOME
ENV PATH $PATH:$HOLOCLEANHOME

EXPOSE 8888 3306

# Run Jupyter
CMD ["jupyter", "notebook", "--port=8888", "--no-browser","--allow-root",  "--ip=0.0.0.0"]
