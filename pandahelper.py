import pandas as pd
import os

class pandahelper:
    
    def __init__(self, data):
        self._df = pd.DataFrame(data)

    def __getattr__(self, name):
        # Delegate attribute access to the underlying DataFrame
        if hasattr(self._df, name):
            return getattr(self._df, name)
        else:
            raise AttributeError(f"'pandahelper' object has no attribute '{name}'")
    
    @classmethod
    def read_file(cls, file_path,**kwargs):
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
        if file_extension in pandas_read_functions:
            try:
                df = pandas_read_functions[file_extension](file_path, **kwargs)
                return cls(df)
            except Exception as e:
                raise ValueError(f"Failed to read the file: {e}")
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        
    def can_convert_to_datetime(self):
        # Checks if a series can be converted to a datetime
        try:
            self._df.to_datetime(self, errors='raise')
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def infer_dtypes(cls,file_path,sample_size = 1000 ):
        # infers the dtypes based on the first 1000 rows in a file. 
        # Override with larger sample for larger datasets
        # Downcasts integers, floats and converts most objects to categories
        # Usful for saving memory on large datasets

        try:
           
            # Step 1: Read a sample of the data
            # Accesses the dataframe in the instance
            df_sample = cls.read_file(file_path, nrows = sample_size)._df
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
        
    def standard_headers_lower(self):
        # standardizes headers to loweracse
        # removes preceding and trailing spaces
        # replaces remaining spaces with underscores
        try:
            self._df.columns = self._df.columns.str.strip().str.lower().str.replace(' ', '_')
            return self  # Return self for chaining
        except Exception as e:
            raise ValueError(f"Failed to standardize headers: {e}")

    def standard_headers_upper(self):
        # standardizes headers to loweracse
        # removes preceding and trailing spaces
        # replaces remaining spaces with underscores
        try:
            self._df.columns = self._df.columns.str.strip().str.upper().str.replace(' ', '_')
            return self  # Return self for chaining
        except Exception as e:
            raise ValueError(f"Failed to standardize headers: {e}")
        
    
