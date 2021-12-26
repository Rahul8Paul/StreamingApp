import pandas as pd
from stream.etl.write_base import WriteBase


class WriteData(WriteBase):
    """Class to read the data from the source"""

    job_type = "write"

    def __init__(self):
        super.__init__(self.job_type)

    def set_class_params(self):
        """Method to set the class params from config file

           Args:

            Returns:
                set the class params
        """
        self.msql = self.config["msql"]
        self.data_db = self.config["data"]["db"]
        self.conn = self.try_mysql_conn()
        self.table = self.config["data"]["table"]

    def execute(self, df: pd.DataFrame):
        """Method as a entry point/execute of this class

           Args:
                df (pd.DataFrame) : pandas row to insert
            Returns:
                NA.
        """
        self.write(df)
