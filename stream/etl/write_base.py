import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from stream.etl.base import Base


class WriteBase(Base):
    """Base class for read write update task, It will be used to read config and define abstract functions"""

    def __init__(self, job_type: str):
        super.__init__(job_type)

    def try_mysql_conn(self):
        """Method to set the class params from config file

           Args:
                NA
            Returns:
                the mysql connection object
        """
        try:
            conn = mysql.connector.connect(user=self.msql["username"], password=self.msql["password"],
                                           host=self.msql["host"],
                                           database=self.data_db)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
                exit(10)
        return conn

    def write(self, df: pd.DataFrame):
        """Method to read the yaml file

           Args:
               df (pd.DataFrame) : pandas dataframe as the input

            Returns:
                Na
        """
        try:
            df.to_sql(self.table, self.conn, if_exists='append', index=True)
        except Exception as e:
            print(e)