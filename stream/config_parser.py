import yml
import pkg_resources

file_name = pkg_resources.resource_filename("config.yaml")
class Config:
    """Class to hold the config object"""
    def __init__(self, job_type):
        yml_dict = self.read_file(file_name)
        yml_dict = yml_dict[job_type]
        self.config = yml_dict

    def read_file(self, file_path):
        """Method to read the yaml file

           Args:
               file_path (str) : path of the yml file in the package

            Returns:
                yml dict
        """
        return yml.load(file_path)