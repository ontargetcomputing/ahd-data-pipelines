from ahd_data_pipelines.integrations.datasource import Datasource
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from arcgis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import Table
import json
import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon, MultiPolygon, shape
from arcgis.geometry import Geometry


class AgolDatasource(Datasource):
    """ """

    def __init__(self, params: dict = None, spark: SparkSession = None):
        self.gis = GIS(params["url"], params["username"], params["password"])
        self.params = params
        self.spark = spark

    def read_from_table(self):
        datasetId = self.params["dataset_id"]
        table_index = self.params["table_index"]
        print(f'loadTable { { "source": datasetId, "table": table_index}}')
        dataLayer = self.gis.content.get(datasetId)
        table = Table(dataLayer.tables[table_index].url)
        records = table.query()
        data = [record.attributes for record in records.features]

        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame(data)
        return PandasHelper.pandas_to_pysparksql(pd_df=df, spark=self.spark)

    def read_from_feature_layer(self):
        datasetId = self.params["dataset_id"]
        dataLayer = self.gis.content.get(datasetId)
        original_crs = self.params.get("original_crs", 3857)
        new_crs = self.params.get("new_crs", 4326)
        layer = self.params["layer"]

        print(f'loadFeatureLayer { { "source": datasetId, "layer": layer}}')

        featureLayer = FeatureLayer(dataLayer.layers[layer].url)
        featureSet = featureLayer.query()
        gdf = featureSet.sdf
        if len(gdf) > 0:
            gjsonString = featureSet.to_geojson
            gjsonDict = json.loads(gjsonString)
            features = gjsonDict["features"]
            data = []
            for feature in features:
                geometry = feature["geometry"]
                if feature["geometry"]["type"] == "MultiPolygon":
                    coords = geometry["coordinates"]
                    polygons = []
                    for poly in coords:
                        points = []
                        for point in poly:
                            points.append((point[0], point[1]))
                        polygons.append(Polygon(points))
                    shp = MultiPolygon(polygons)
                else:
                    shp = shape(geometry)

                props = feature["properties"]
                props["geometry"] = shp
                data.append(props)

            df = pd.DataFrame(data)
            # columns_to_drop = ['SHAPE', 'Shape__Area', 'Shape__Length']

            # # Check if each column exists before dropping it
            # for column in columns_to_drop:
            #     if column in df.columns:
            #         df.drop(columns=column, inplace=True)

            transformed = gpd.GeoDataFrame(df, geometry="geometry")

            geom = transformed["geometry"]

            gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(gdf, crs=f"EPSG:{original_crs}", geometry=geom)
            gdf = gdf.set_crs(f"EPSG:{original_crs}")

            if original_crs != new_crs:
                gdf = gdf.to_crs(f"EPSG:{new_crs}")
            return PandasHelper.geopandas_to_pysparksql(gpd_df=gdf, spark=self.spark)
        else:
            return PandasHelper.empty_spark_sql_dataframe(spark=self.spark)

    def read(self) -> DataFrame:
        if self.params is None:
            raise TypeError("params:NoneType not allowed, params:dict exptected")

        is_table = self.params.get("is_table", False)
        if is_table:
            return self.read_from_table()
        else:
            return self.read_from_feature_layer()

    def write_to_feature_layer(self, dataFrame: DataFrame):
        datasetId = self.params["dataset_id"]
        layer = self.params["layer"]
        print(f'writing { { "source": datasetId, "layer": layer}}')

        dataLayer = self.gis.content.get(datasetId)
        featureLayer = FeatureLayer(dataLayer.layers[layer].url)

        self.truncate_feature_layer()
        features = []
        dataFrame = dataFrame.toPandas()
        dataFrame = dataFrame.astype(object).where(pd.notnull(dataFrame), None)
        dataFrame["geometry"] = dataFrame["geometry"].apply(wkt.loads)

        for index, row in dataFrame.iterrows():
            the_dict = row.to_dict()
            wrappedObj = {"attributes": the_dict}
            if "geometry" in self.params:
                column = self.params["geometry"]["column"]
                geometry = the_dict[column]
                del the_dict[column]
                the_type = self.params["geometry"]["type"]
                print(f"Converting geometry type:{type}")
                if the_type == "DYNAMIC":
                    #geom_type = type(geometry)
                    if isinstance(geometry, MultiPolygon):
                        the_type = "MULTIPART_POLYGON"
                    elif isinstance(geometry, Polygon):
                        the_type = "POLYGON"
                    elif isinstance(geometry, Point):
                        the_type = "POINT"
                    else:
                        the_type = type(geometry)
                    #print(type(geometry))
                    #print(geometry)

                if the_type == "POINT":
                    wrappedObj["geometry"] = {
                        "x": geometry.x,
                        "y": geometry.y,
                        "spatialReference": {"wkid": 4326},
                    }
                elif the_type == "POLYGON":
                    x, y = geometry.exterior.coords.xy
                    x = list(x)
                    y = list(y)
                    wrappedObj["geometry"] = {
                        "rings": [[list(z) for z in zip(x, y)]],
                        "spatialReference": {"wkid": 4326},
                    }
                elif the_type == "MULTIPART_POLYGON":
                    wrappedObj["geometry"] = Geometry(geometry.__geo_interface__)
                else:
                    print(f"Unknown geometry type:{the_type}")

            features.append(wrappedObj)

        print("Writing new features")
        result = featureLayer.edit_features(adds=features)
        print(f"Wrote new features - {result}")

    def write_to_table(self, dataFrame: DataFrame):
        datasetId = self.params["dataset_id"]
        table_index = self.params["table_index"]
        print(f'writing { { "source": datasetId, "table_index": table_index}}')

        dataLayer = self.gis.content.get(datasetId)
        table = Table(dataLayer.tables[table_index].url)
        self.truncate_table()
        rows = []
        dataFrame = dataFrame.toPandas()
        dataFrame = dataFrame.astype(object).where(pd.notnull(dataFrame), None)

        for index, row in dataFrame.iterrows():
            the_dict = row.to_dict()
            wrappedObj = {"attributes": the_dict}
            rows.append(wrappedObj)

        print("Writing new table")
        result = table.edit_features(adds=rows)
        print(f"Wrote new table - {result}")

    def write(self, dataFrame: DataFrame):
        is_table = self.params.get("is_table", False)
        if is_table:
            return self.write_to_table(dataFrame)
        else:
            return self.write_to_feature_layer(dataFrame)

    def truncate_feature_layer(self):
        datasetId = self.params["dataset_id"]
        layer = self.params["layer"]
        objectid = self.params.get("object_id", "OBJECTID")
        print(f'truncateFeatureLayer { { "source": datasetId, "layer": layer, "object_id": objectid}}')
        dataLayer = self.gis.content.get(datasetId)
        featureLayer = FeatureLayer(dataLayer.layers[layer].url)
        # featureLayer.delete_features()
        result = featureLayer.delete_features(where=f"{objectid} > 0")
        print(result)

    def truncate_table(self):
        datasetId = self.params["dataset_id"]
        table_index = self.params["table_index"]
        objectid = self.params.get("object_id", "OBJECTID")
        print(f'truncateTable { { "source": datasetId, "table_index": table_index, "object_id": objectid}}')
        dataLayer = self.gis.content.get(datasetId)
        table = Table(dataLayer.tables[table_index].url)

        result = table.delete_features(where=f"{objectid} > 0")
        print(result)

    def truncate(self):
        is_table = self.params.get("is_table", False)
        if is_table:
            return self.truncate_table()
        else:
            return self.truncate_feature_layer()
