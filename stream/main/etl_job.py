from stream.etl.base import Base
from stream.etl.read import ReadData
from stream.etl.write import WriteData
from stream.etl.update_stat import UpdateStat
from time import sleep


class EtlJob(Base):
    """Class responsible for reading the data and loading to mysql and update statistics table"""

    job_name = "etl_job"

    def __init__(self):
        super.__init__(self.job_name)

    def set_class_params(self):
        """Method to set the class params from config file

           Args:

            Returns:
                set the class params
        """
        self.time_gap = self.config["time_gap"]

    def execute(self):
        """Method to set the class params from config file

            Args:

            Returns:
                execute the who etl process of read load and update stat
        """
        while True:
            read = ReadData()
            df = read.execute()
            write = WriteData()
            write.execute(df)
            update_stat = UpdateStat()
            update_stat.execute(df)
            sleep(self.time_gap)

