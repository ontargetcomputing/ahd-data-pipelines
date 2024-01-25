from ahd_data_pipelines.transformations.transformation import Transformation


def entrypoint():  # pragma: no cover
    for cls in Transformation.__subclasses__():
        cls.transform(dataFrame=None)


if __name__ == '__main__':
    entrypoint()
