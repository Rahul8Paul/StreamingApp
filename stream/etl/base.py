from stream.config_parser import Config


class Base:
    """Base class for read write update task, It will be used to read config and define abstract functions"""

    def __init__(self, job_type: str):
        print("Base class got initialized")
        self.config = Config(job_type)
        self.set_class_params()

    @abstract
    def set_class_params(self):
        pass
