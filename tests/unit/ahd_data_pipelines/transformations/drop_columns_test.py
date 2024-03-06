from ahd_data_pipelines.transformations.drop_columns import DropColumns



# comment on this test so people understand what you are doing.
def test_to_perform_returns_true_appropriately():
    #setup
    params = {
      "drop_columns": [
        "days_within"
      ]
    }

    #execute
    test_value = DropColumns.to_perform(params)

    #execute
    assert test_value == True


def test_to_perform_returns_false_appropriately():
    #setup
    params = {
      "xyz_columns": [
        "days_within"
      ]
    }

    #execute
    test_value = DropColumns.to_perform(params)
    
    # validate
    assert test_value == False

# def test_execute_works_with_single():
    
#     assert True == False

# def test_execute_with_multiple():
#     assert True == False

# def test_execute_works_with_nothing():
#     assert True == False
