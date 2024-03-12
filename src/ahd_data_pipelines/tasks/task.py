from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Dict, Any
import yaml
import pathlib
from pyspark.sql import SparkSession
from delta import *  # noqa
from delta.tables import *  # noqa
import sys
import os

# import json


def get_dbutils(
    spark: SparkSession,
):  # please note that this function is used in mocking by its name
    try:
        if os.environ.get("LOCAL") == "true":
            return None
        else:
            from pyspark.dbutils import DBUtils  # noqa

            if "dbutils" not in locals():
                utils = DBUtils(spark)
                return utils
            else:
                return locals().get("dbutils")
    except ImportError:
        return None


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        if "true" != os.environ.get("LOCAL"):
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._determine_stage()
        # self._determine_job_metadata()
        self._log_conf()

    def to_string(self):
        return f"Job:{self.jobName}, Task:{self.taskKey}, Run:{self.runNum}"

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            if "true" != os.environ.get("LOCAL"):
                return SparkSession.builder.getOrCreate()
            else:
                if "true" == os.environ.get("CI"):
                    from delta import configure_spark_with_delta_pip

                    builder = (
                        SparkSession.builder.appName("MyApp")
                        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                        .config(
                            "spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension",
                        )
                        .config(
                            "spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        )
                    )
                    return configure_spark_with_delta_pip(builder).getOrCreate()
                else:
                    from databricks.connect import DatabricksSession
                    from databricks.sdk.core import Config

                    config = Config(
                        host=os.environ.get("DB_HOST"),
                        token=os.environ.get("DB_TOKEN"),
                        cluster_id=os.environ.get("DB_CLUSTER_ID"),
                    )
                    return DatabricksSession.builder.sdkConfig(config).getOrCreate()
        else:
            return spark

    def get_dbutils(self):
        utils = get_dbutils(self.spark)

        if not utils:
            print("No DBUtils defined in the runtime")
        else:
            print("DBUtils class initialized")

        return utils

    def _provide_config(self):
        print("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            print(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            print(f"Conf file was provided, reading configuration from {conf_file}")
            return self._read_config(conf_file)

    @staticmethod
    def _get_conf_file():
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        self.log4j_logger = log4j_logger
        thelogger = log4j_logger.LogManager.getLogger(self.__class__.__name__)
        if os.environ.get("LOCAL") == "true":
            thelogger.setLevel(log4j_logger.Level.DEBUG)

        return thelogger

    def _log_conf(self):
        # log parameters
        print("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            print("\t Parameter: %-30s with value => %-30s" % (key, item))

    def _determine_stage(self):
        p = ArgumentParser()
        p.add_argument("--stage", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        if namespace.stage is None:
            self.stage = "DEV"
        else:
            self.stage = namespace.stage.upper()
        print(f"Determined stage={self.stage} from --stage job option")

    # def _determine_job_metadata(self):
    #     # TODO fix this....not working on run from dbx
    #     if False:
    #         if self.dbutils is None or os.environ.get('LOCAL') == 'true':
    #             self.jobName = 'local'
    #             self.taskKey = 'local'
    #             self.runNum = 'local'
    #         else:
    #             context = json.loads(self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    #             tags = context['tags']
    #             if tags['jobName'] is not None:
    #                 jobName = tags['jobName']
    #                 print(tags)
    #                 self.jobName = tags['jobName']
    #                 self.taskKey = tags['taskKey']
    #                 self.runNum = tags['multitaskParentRunId']
    #             else:
    #                 self.jobName = 'local-dbx'
    #                 self.taskKey = 'local-dbx'
    #                 self.runNum = 'local-dbx'

    @abstractmethod
    def launch(self):
        """
        Main method of the job.
        :return:
        """
        pass
