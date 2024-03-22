from ahd_data_pipelines.integrations.http_processors.http_processor import HTTPProcessor
import yaml
import json

class NationalWeatherServiceHttpProcessor(HTTPProcessor):
    """ """

    def process(self, params, spark=None):
        endpoint = params["endpoint"]
        response = self.get_http_response(endpoint)
        features = response.json()['features']
        json_objects = []

        for feature in features:
            json_object = {
                "Type": feature['properties']['event'],
                "Severity": feature['properties']['severity'],
                "Summary": feature['properties']['headline'],
                "Link": feature['properties']['@id'],
                "Urgency": feature['properties']['urgency'],
                "Certainty": feature['properties']['certainty'],
                "Category": feature['properties']['category'],
                "UpdatedDate": feature['properties']['sent'],
                "EffectiveDate": feature['properties']['effective'],
                "ExpirationDate": feature['properties']['expires'],
                "UniqueId": feature['properties']['id'],
                "AreasAffected": feature['properties']['areaDesc'],
                "AreaIdsAffected":  ", ".join(feature['properties']['geocode']['UGC']),
                "Geometry": json.dumps(feature['geometry'])
            }
            # Append the JSON object to the list
            json_objects.append(json_object)

        return json_objects
    

if __name__ == "__main__":
    yaml_file_path = "./conf/parameters/hazard/weather/bronze.yml"
    with open(yaml_file_path, "r") as file:
        conf = yaml.safe_load(file)    
    
    print("******************************")
    print("LOADED CONFIG FROM YAML FILE LOCALLY")
    print(json.dumps(conf, indent=2))
    print("******************************")

    processor = NationalWeatherServiceHttpProcessor()

    processor.process(params=conf['source_datasources'][0])
