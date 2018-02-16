FROM bosr/pytorch
# FROM digitalgenius/ubuntu-pytorch
MAINTAINER HoloClean "holo@clean.org"

# Copy all the required folders into the docker image
COPY python-package-requirement.txt /holoclean/
COPY holoclean /holoclean/holoclean/
COPY test /holoclean/test/
COPY script.py 	/holoclean/

WORKDIR /holoclean
# Install VIM, not really needed
RUN apt-get install -y vim

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

