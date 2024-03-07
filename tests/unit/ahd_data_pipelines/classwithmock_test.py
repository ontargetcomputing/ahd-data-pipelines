from ahd_data_pipelines.classwithmock import uppercasethename
from unittest.mock import patch
import ahd_data_pipelines.uppercase



# comment on this test so people understand what you are doing.
def test_with_mock():
    with patch('ahd_data_pipelines.uppercase') as mock_uppercase:
        # Mocking the return value of requests.get
        mock_uppercase.return_value = "JEFF"

        # Calling the function under test
        result = uppercasethename("jeff")

        # Asserting that the function behaves as expected
        assert result == "JEFF"
