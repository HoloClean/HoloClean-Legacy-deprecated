#!/bin/bash


sudo mysql  -p <<MYSQL_SCRIPT
CREATE DATABASE holo;
create user 'holocleanUser'@'localhost' identified by 'abcd1234';
GRANT ALL PRIVILEGES ON holo.* TO 'holocleanUser'@'localhost';
FLUSH PRIVILEGES;
MYSQL_SCRIPT

echo "MySQL database and user has been created."

