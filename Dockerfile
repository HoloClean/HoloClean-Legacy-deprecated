FROM digitalgenius/ubuntu-pytorch
MAINTAINER HoloClean "holo@clean.org"

# Install MySQL
RUN apt-get update && apt-get install -y mysql-server mysql-client
RUN mysql -e "use mysql; update user set authentication_string=PASSWORD('') where User='root'; update user set plugin='mysql_native_password';FLUSH PRIVILEGES;"
RUN service mysql restart
RUN mysql_upgrade --force 

RUN mysql -u root -e "CREATE DATABASE holo;"
RUN mysql -u root -e "Create user 'holocleanUser'@'localhost' identified by 'abcd1234'";
RUN mysql -u root -e "GRANT ALL PRIVILEGES ON holo.* TO 'holocleanUser'@'localhost'";

