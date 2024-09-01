import pandas as pd
import os


def read_file(file_path,**kwargs):
    # Automatically pulls the appropriate pandas read function
    # Requires all necessary keyword arguments to be passed
    # Requires all relevant dependencies to run

    pandas_read_functions = {
    ".csv": pd.read_csv,
    ".xlsx": pd.read_excel,
    ".json": pd.read_json,
    ".html": pd.read_html,
    ".sql": pd.read_sql,
    ".sql_table": pd.read_sql_table,
    ".sql_query": pd.read_sql_query,
    ".parquet": pd.read_parquet,
    ".orc": pd.read_orc,
    ".stata": pd.read_stata,
    ".sas": pd.read_sas,
    ".pickle": pd.read_pickle,
    ".clipboard": pd.read_clipboard,
    ".hdf": pd.read_hdf,
    ".feather": pd.read_feather,
    ".gbq": pd.read_gbq,
    ".xml": pd.read_xml
}
    _, file_extension = os.path.splitext(file_path)
    try:
        df = pandas_read_functions[file_extension](file_path, **kwargs)
        return df
    except:
        raise


def infer_dtypes(file_path, sample_size = 1000):
    # infers the dtypes based on the first 1000 rows in a file. Override with larger sample for larger datasets
    # Downcasts integers, floats and converts most objects to categories
    # Usful for saving memory on large datasets

    try:
        # Step 1: Read a sample of the data
        df_sample = read_file(file_path, nrows = sample_size)
        # Step 2: Infer dtypes based on the sample
        inferred_dtypes = {}
        for column in df_sample.columns:
            # Infer dtype
            inferred_dtype = df_sample[column].dtype
            # Downcast numerical types to save memory
            if pd.api.types.is_integer_dtype(inferred_dtype):
                inferred_dtype = pd.to_numeric(df_sample[column], downcast='integer').dtype
            elif pd.api.types.is_float_dtype(inferred_dtype):
                inferred_dtype = pd.to_numeric(df_sample[column], downcast='float').dtype
            elif pd.api.types.is_object_dtype(inferred_dtype) and df_sample[column].nunique() < len(df_sample[column]) * 0.5:
                inferred_dtype = 'category'
            # Store the inferred dtype
            inferred_dtypes[column] = inferred_dtype
        return inferred_dtypes
    except:
        raise


