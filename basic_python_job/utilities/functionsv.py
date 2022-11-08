from great_expectations.dataset import SparkDFDataset

def exists_columns(col,df):
    raw_test_df = SparkDFDataset(df)
    for i in col:

        assert raw_test_df.expect_column_to_exist(i).success, f" Column {i} does not exist: FAILED"
        #print(f"Column {i} exists: PASSED")


def columns_without_null(col,df):
    raw_test_df = SparkDFDataset(df)
    for i in col:
        test_result = raw_test_df.expect_column_values_to_not_be_null(i)

        assert test_result.success,\
        f"{test_result.result['unexpected_count']} of {test_result.result['element_count']} in Column {i} are null: FAILED"
        #print(f"Column {i} does have null: PASSED")

def columns_values_between(col,df,min_val,max_val):
    raw_test_df = SparkDFDataset(df)
    for i in col:
        test_result = raw_test_df.expect_column_values_to_be_between(i,min_val,max_val)
        assert test_result.success,\
        f"{test_result.result['unexpected_count']} of {test_result.result['element_count']} in Column {i} are null: FAILED"
        #print(f"Column {i} does have null: PASSED")


def validate_dtype(dic,df):
    
    """
    Should be insert a dict with mapping of the data
    """

    raw_test_df = SparkDFDataset(df)
    for i,j in dic.items():
        test_result = raw_test_df.expect_column_values_to_be_of_type(i,j)

        assert test_result.success, \
        f"{i} Column is {test_result.result['observed_value']} and must be {j}: FAILEDed "
        #print(f"{i} Column is {j}: PASSED")

def columns_unique_values(col,df):
    
    raw_test_df = SparkDFDataset(df)

    for i in col:

        assert raw_test_df.expect_column_unique_value_count_to_be_between(i), f"{i} Column does not have unique values"
        #print(f"{i} Column has unique values")