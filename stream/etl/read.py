import pandas as pd
from stream.etl.base import Base


class ReadData(Base):
    """Class to read the data from the source"""

    job_type = "read"

    def __init__(self):
        super.__init__(self.job_type)

    def set_class_params(self):
        """Method to set the class params from config file

           Args:

            Returns:
                set the class params
        """
        self.data_path = self.config["path"]
        self.time_gap = self.config["time_gap"]
        self.index = 0

    def read_data(self, df: pd.DataFrame):
        """Method to read the yaml file

           Args:
               df (pd.DataFrame) : pandas dataframe as the input

            Returns:
                row of the dataframe
        """
        try:
            result = df[df.index == self.index]
            return result
        except Exception as e:
            print(e)

    def execute(self):
        """Method as a entry point/execute of this class

           Args:

            Returns:
                a particular row of the datafrmae.
        """
        df = pd.read_excel(self.data_path)
        self.index += 1
        return self.read_data(df)
