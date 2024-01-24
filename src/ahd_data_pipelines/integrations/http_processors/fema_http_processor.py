from cdii_data_pipelines.integrations.http_processors.http_processor import HTTPProcessor
import re
from datetime import datetime, timedelta


class FEMAHTTPProcessor(HTTPProcessor):
    """
    """

    def process(self, params, spark=None):
        endpoint = params['endpoint']
        query_parameters = params['query_parameters']

        constructed_query_parameters = self.construct_query_params(query_parameters)

        more_pages = True
        if 'paginate' in params.keys():
            paginate_query_parameter = params['paginate']['query_parameter']
            page_size = params['paginate']['page_size']

            page_num = 1
            all_data = []
            while more_pages is True:
                page = self.get_page(
                    endpoint,
                    constructed_query_parameters,
                    paginate_query_parameter,
                    page_num,
                    page_size)
                if len(page) > 0:
                    all_data.extend(page)
                if len(page) < page_size:
                    print(f'There are {len(all_data)} records and there are NO more pages')
                    more_pages = False
                else:
                    print(f'There are {len(all_data)} records and there are more pages')
                    page_num = page_num + 1

        else:
            raise NotImplementedError('No non pagination implementation is done yet')

        return all_data

    def construct_query_params(self, query_parameters):
        constructed_query_params = query_parameters
        pattern = r"({[^{]*})"

        matches = re.findall(pattern, query_parameters)

        # TODO : clean this up....this is a bit sloppy.
        for match in matches:

            terms = (match[1:len(match) - 1]).split(':', 1)
            if terms[0] == 'today':
                today_formatted = datetime.today().strftime(terms[1])
                constructed_query_params = constructed_query_params.replace(match, today_formatted)
                print(f'Replaced {match} with {today_formatted}')
            elif terms[0] == 'yesterday':
                yesterday = datetime.today() - timedelta(days=1)
                yesterday_formatted = yesterday.strftime(terms[1])
                # yesterday_formatted = '2022-05-09T00:00:00.000z'
                constructed_query_params = constructed_query_params.replace(match, yesterday_formatted)
                print(f'Replaced {match} with {yesterday_formatted}')
            else:
                raise Exception(f'Unknown term in query parameters : {terms[0]}')

        print(f'Constructed query parameters are {constructed_query_params}')

        return constructed_query_params

    def get_page(self, endpoint, query_parameters, paginate_query_parameter, page_num, page_size):
        url = f'{endpoint}?{query_parameters}&{paginate_query_parameter}={(page_num-1)*page_size}'
        print(f'Url = {url}')

        response = self.get_http_response(url)
        json_data = response.json()
        records_entity = json_data['metadata']['entityname']
        records = json_data[records_entity]
        return records
