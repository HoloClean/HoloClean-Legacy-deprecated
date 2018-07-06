#!/bin/bash

sudo -u postgres psql <<PGSCRIPT

DROP database holo;
CREATE database holo;
CREATE user holocleanuser;
ALTER USER holocleanuser WITH PASSWORD 'abcd1234';
GRANT ALL PRIVILEGES on database holo to holocleanUser ;
\c holo
ALTER SCHEMA public OWNER TO holocleanUser;
PGSCRIPT

echo "PG database and user has been created."

