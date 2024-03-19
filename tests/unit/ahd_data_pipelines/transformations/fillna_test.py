import unittest
import pandas as pd
from ahd_data_pipelines.transformations.transformation import Transformation
from ahd_data_pipelines.transformations.fillna import FillNa

class TestFillNaTransformation(unittest.TestCase):
    def test_to_perform(self):
        # Test when 'fillna' key exists in params
        params_with_fillna = {'fillna': 0}
        self.assertTrue(FillNa.to_perform(params_with_fillna))
        
        # Test when 'fillna' key does not exist in params
        params_without_fillna = {'other_param': 0}
        self.assertFalse(FillNa.to_perform(params_without_fillna))

    def test_execute(self):
        # Setup
        df = pd.DataFrame({'A': [1, 2, None, 4]})
        params = {'fillna': 0}
        
        # Execute
        result_df = FillNa.execute(df, params)

        # Assert
        assert result_df['A'][2] == 0
        
if __name__ == '__main__':
    unittest.main()
