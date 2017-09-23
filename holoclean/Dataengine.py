import pandas as pd
import numpy as np
import mysql.connector
import sqlalchemy as sqla
import getpass
import logging


class dataengine:

    """Connects our program to the database

    """

    def __init__(self):
        logging.basicConfig(filename='dataengine.log', level=logging.DEBUG)
        pass

    def connect(self):
        """create a connection with the database"""
        user = raw_input("please write the user for the mysql database: ")
        password = getpass.getpass("please write the password for" +
                                   "the mysql database: ")
        address = raw_input("please write address for the mysql database: ")
        self.engine = sqla.create_engine("mysql+mysqldb://" + user +
                                         ":"+password+"@"+address+"/holoclean")

    def register(self, chunk):
        """for the first chunk, create the table and return
           the name of the table"""
        name_table = raw_input("please write the name of the table for" +
                               "the mysql database: ")
        try:
            chunk.to_sql(name_table, con=self.engine, if_exists='append',
                         index=True, index_label=None)
            logging.info("correct insertion for" + name_table)
        except sqla.exc.IntegrityError:
            logging.warn("failed to insert values")
        return name_table

    def add(self, chunk, name_table):
        """adding the information from the chunk to the table"""
        try:
            chunk.to_sql(name_table, con=self.engine, if_exists='append',
                         index=True, index_label=None)
            logging.info("correct insertion for" + name_table)
        except sqla.exc.IntegrityError:
            logging.warn("failed to insert values")
