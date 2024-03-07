#from ahd_data_pipelines.classwithmock import uppercasethename
import ahd_data_pipelines.classwithmock
from unittest.mock import patch
#import ahd_data_pipelines.uppercase

def test_with_mock():
    with patch('ahd_data_pipelines.classwithmock.uppercasethename') as mock_uppercasethename:
        # Mocking the return value of 
        mock_uppercasethename.return_value = "JEFFX"

        # Calling the function under test
        print(ahd_data_pipelines.classwithmock.uppercasethename("jeff1"))
        print(ahd_data_pipelines.classwithmock.uppercasethename("jeff2"))
        print(ahd_data_pipelines.classwithmock.uppercasethename("jeff3"))
        print(mock_uppercasethename.return_value)
        result = ahd_data_pipelines.classwithmock.uppercasethename("ccc")

        # Asserting that the function behaves as expected
        assert result == "JEFFX"
