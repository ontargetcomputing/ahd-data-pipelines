from ahd_data_pipelines.tasks.esri.green_data_update import GreenDataUpdate
from ahd_data_pipelines.tasks.legacy.job import Job


class GreenDataUpdateJob():
    def launch(self):
        self.logger.info("Launching Green Data Update job")
        self.pipeline = GreenDataUpdate(
            conf=None,
            dbutils=None,
            development=True,
            log4j_logger=None,
            stage='dev',
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
