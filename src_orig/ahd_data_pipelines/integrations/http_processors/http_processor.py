from abc import ABC, abstractmethod
import requests


class HTTPProcessor(ABC):
    """
    """
    @abstractmethod
    def process(self, params, spark=None):
        pass

    def get_http_response(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            # If the request was successful (status code 200),
            # the JSON data can be retrieved from the response's 'json' method
            return response
        else:
            raise Exception(f"Request failed with status code:{response.status_code}")
