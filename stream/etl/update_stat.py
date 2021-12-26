import pandas as pd
from stream.etl.write_base import WriteBase
from datetime import datetime


class UpdateStat(WriteBase):
    """Class to read the data from the source"""

    job_type = "update"

    def __init__(self):
        super.__init__(self.job_type)

    def set_class_params(self):
        """Method to set the class params from config file

           Args:

            Returns:
                set the class params
        """
        self.msql = self.config["msql"]
        self.data_db = self.config["statistics"]["db"]
        self.conn = self.try_mysql_conn()
        self.table = self.config["statistics"]["table"]
        self.cols = self.config["statistics"]["cols"]
        self.stat = {}
        for col in self.cols:
            self.stat[col] = {"max": -1, "count": 0, "min": -1, "sum": 0, "avg": 0.0}

    def update_stat_dict(self, df: pd.DataFrame):
        """Method to update the max for the fields

           Args:
               df (pd.DataFrame) : pandas dataframe as the input

            Returns:
                Na
        """
        for col in self.col:
            val = df[col].iloc[0]
            if self.stat[col]["min"] == -1:
                self.stat[col]["min"] = val
            elif val < self.stat[col]["min"]:
                self.stat[col]["min"] = val
            if val > self.stat[col]["max"] :
                self.stat[col]["max"] = val
            self.stat[col]["count"] += 1
            self.stat[col]["sum"] += val
            self.stat[col]["avg"] = self.stat[col]["sum"] / self.stat[col]["count"]

    def execute(self, df: pd.DataFrame):
        """Method as a entry point/execute of this class

           Args:
                df (pd.DataFrame) : pandas row to update the existing statistics
            Returns:
                NA.
        """
        self.update_stat_dict(df)
        df_stat = pd.DataFrame(self.stat)
        df_stat["timestamp"] = datetime.now()
        self.write(df_stat)
