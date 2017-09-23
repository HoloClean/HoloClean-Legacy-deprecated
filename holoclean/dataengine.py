import pandas as pd
import numpy as np
import sqlalchemy as sqla
import getpass
import logging


class dataengine:

    """Connects our program to the database

    """

    def __init__(self):
        logging.basicConfig(filename='dataengine.log', level=logging.DEBUG)
        pass

    def connect(self,filepath):
        """create a connection with the database"""
        file = open(filepath,"r")
        
        address = file.readline()
        dbname = file.readline()
        user = file.readline()
        password = file.readline()
        con_str="mysql+mysqldb://" + user[:-1] +":"+password[:-1]+"@"+address[:-1]+"/"+dbname[:-1]
        self.engine = sqla.create_engine(con_str)

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
            
d=dataengine()
d.connect('config.txt')
print("Done!")

