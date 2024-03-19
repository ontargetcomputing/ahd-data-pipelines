from ahd_data_pipelines.tasks.esri.green_data_update import GreenDataUpdate
from ahd_data_pipelines.tasks.legacy.job import Job


class GreenDataUpdateJob(Job):
    def launch(self):
        self.logger.info("Launching Green Data Update job")
        self.pipeline = GreenDataUpdate(
            conf=self.conf,
            dbutils=self.dbutils,
            development=self._is_development(),
            log4j_logger=self.log4j_logger,
            stage=self.stage,
        )

        self.pipeline.run()
        self.logger.info("Green Data Update finished!")


def entrypoint():
    # pragma: no cover
    print("Running Green Data Update")

    job = GreenDataUpdateJob()
    job.launch()


if __name__ == "__main__":
    entrypoint()
