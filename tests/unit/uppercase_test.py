from ahd_data_pipelines.uppercase import uppercase

# comment on this test so people understand what you are doing.
def test_correct_return_value_is_given_when_jeff_is_the_value():
    # setup
    str = "jeff"

    # execute
    return_value = uppercase(str)

    # validate
    assert return_value == str.upper()

def test_correct_return_value_is_given_when_something_other_than_jeff_is_given():
    # setup
    str = "richard"

    # execute
    return_value = uppercase(str)

    # validate
    assert return_value == str.upper()
