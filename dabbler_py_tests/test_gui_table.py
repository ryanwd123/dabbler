# #%%
# import pytest
# import polars as pl
# from dabbler.gui_table import build_filter, get_columns_for_filter
# from datetime import datetime, date

# sample_df1 = pl.DataFrame({
#         'A': [1, 2, 3, 4, 5],
#         'B': ['apple', 'banana', 'cherry', 'date', 'elderberry'],
#         'C': pl.Series(['red', 'yellow', 'red', 'brown', 'purple'], dtype=pl.Categorical),
#         'D': [10.5, 20.3, 30.7, 40.2, 50.1],
#         'E': [True, False, True, False, True],
#         'F': [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1), date(2023, 4, 1), date(2023, 5, 1)],
#         'G': [datetime(2023, 1, 1, 12, 0), datetime(2023, 2, 1, 13, 0), datetime(2023, 3, 1, 14, 0),
#               datetime(2023, 4, 1, 15, 0), datetime(2023, 5, 1, 16, 0)],
#         'H': [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]],
#         'I': [{'a': 1}, {'b': 2}, {'c': 3}, {'d': 4}, {'e': 5}]
#     })




# #%%

# @pytest.fixture
# def sample_df():
#     return sample_df1

# def test_get_columns_for_filter(sample_df):
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     assert set(non_categorical) == {'A', 'B', 'D', 'E', 'F', 'G'}
#     assert set(categorical) == {'C'}

# def test_build_filter_simple_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'apple', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 1
#     assert result['B'][0] == 'apple'

# def test_build_filter_numeric_greater_than(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '>3', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 2
#     assert all(result['A'] > 3)

# def test_build_filter_numeric_less_than(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '<30', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 2
#     assert all(result['D'] < 30)

# def test_build_filter_negative_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '-apple', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 4
#     assert all(result['B'] != 'apple')

# def test_build_filter_categorical_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'red', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 2
#     assert all(result['C'] == 'red')

# def test_build_filter_boolean_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'true', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 3
#     assert all(result['E'] == True)

# def test_build_filter_date_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '2023-03-01', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 1
#     assert result['F'][0] == date(2023, 3, 1)

# def test_build_filter_datetime_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '2023-04-01', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 1
#     assert result['G'][0] == datetime(2023, 4, 1, 15, 0)

# def test_build_filter_list_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '5', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 2
#     assert any(5 in row for row in result['H'])

# def test_build_filter_dict_search(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'c', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 1
#     assert 'c' in result['I'][0]

# def test_build_filter_multiple_terms(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'apple', non_categorical, categorical)
#     build_filter(filters, 'red', non_categorical, categorical)
#     result = sample_df.filter(pl.all(filters))
#     assert result.shape[0] == 1
#     assert result['B'][0] == 'apple'
#     assert result['C'][0] == 'red'

# def test_build_filter_no_match(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'nonexistent', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 0

# def test_build_filter_case_insensitive(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, 'APPLE', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 1
#     assert result['B'][0] == 'apple'

# def test_build_filter_numeric_decimal(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '>20.5', non_categorical, categorical)
#     result = sample_df.filter(filters[0])
#     assert result.shape[0] == 3
#     assert all(result['D'] > 20.5)

# def test_build_filter_empty_term(sample_df):
#     filters = []
#     non_categorical, categorical = get_columns_for_filter(sample_df)
#     build_filter(filters, '', non_categorical, categorical)
#     assert len(filters) == 0

# # Additional tests for get_columns_for_filter function

# def test_get_columns_for_filter_no_categorical():
#     df = pl.DataFrame({
#         'A': [1, 2, 3],
#         'B': ['a', 'b', 'c'],
#         'C': [1.1, 2.2, 3.3]
#     })
#     non_categorical, categorical = get_columns_for_filter(df)
#     assert set(non_categorical) == {'A', 'B', 'C'}
#     assert len(categorical) == 0

# def test_get_columns_for_filter_only_categorical():
#     df = pl.DataFrame({
#         'A': pl.Series(['red', 'blue', 'green'], dtype=pl.Categorical),
#         'B': pl.Series(['small', 'medium', 'large'], dtype=pl.Categorical)
#     })
#     non_categorical, categorical = get_columns_for_filter(df)
#     assert len(non_categorical) == 0
#     assert set(categorical) == {'A', 'B'}

# def test_get_columns_for_filter_with_list_and_struct():
#     df = pl.DataFrame({
#         'A': [1, 2, 3],
#         'B': ['a', 'b', 'c'],
#         'C': pl.Series(['red', 'blue', 'green'], dtype=pl.Categorical),
#         'D': [[1, 2], [3, 4], [5, 6]],
#         'E': [{'x': 1}, {'y': 2}, {'z': 3}]
#     })
#     non_categorical, categorical = get_columns_for_filter(df)
#     assert set(non_categorical) == {'A', 'B'}
#     assert set(categorical) == {'C'}

# def test_get_columns_for_filter_empty_dataframe():
#     df = pl.DataFrame()
#     non_categorical, categorical = get_columns_for_filter(df)
#     assert len(non_categorical) == 0
#     assert len(categorical) == 0