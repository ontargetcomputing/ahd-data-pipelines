from abc import ABC, abstractmethod
import os


class Pipeline(ABC):
    def __init__(self, dbutils=None, stage="DEV", log4j_logger=None):
        print(f"Launching in {stage}")
        self.stage = stage
        if dbutils is None:
            self.db_user = os.environ.get("RD_OPTION_DB_USER")
            self.db_password = os.environ.get("RD_OPTION_DB_PASSWORD")
            self.agol_user = os.environ.get("RD_OPTION_AGOL_USER")
            self.agol_password = os.environ.get("RD_OPTION_AGOL_PASSWORD")
        else:
            self.db_user = dbutils.secrets.get("SECRET_KEYS", f"JDBC_USERNAME_{stage}")
            self.db_password = dbutils.secrets.get("SECRET_KEYS", f"JDBC_PASSWORD_{stage}")
            self.agol_user = dbutils.secrets.get("SECRET_KEYS", f"AGOL_USERNAME_{stage}")
            self.agol_password = dbutils.secrets.get("SECRET_KEYS", f"AGOL_PASSWORD_{stage}")

        if log4j_logger is None:
            raise Exception("No Logger Configured")

        self.logger = log4j_logger.LogManager.getLogger(self.__class__.__name__)
        if os.environ.get("DEVELOPMENT") == "true":
            self.logger.setLevel(log4j_logger.Level.DEBUG)

    @abstractmethod
    def run(self):
        """
        Main method of the pipeline.
        :return:
        """
        pass
