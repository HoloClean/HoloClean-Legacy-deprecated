#!/bin/bash

pg_ctl -D /Users/ilyas/HC/pg-holo -l logfile start

psql <<PGSCRIPT
 
CREATE database holo;
CREATE user holocleanuser;
ALTER USER holocleanuser WITH PASSWORD 'abcd1234';
GRANT ALL PRIVILEGES on database holo to holocleanUser ;
PGSCRIPT

echo "PG database and user has been created."

