from datetime import datetime
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
        """
        Automatically pulls the appropriate pandas read function
        Requires all necessary keyword arguments to be passed
        Requires all relevant dependencies to run
        """
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
        """
        infers the dtypes based on the first 1000 rows in a file. 
        Override with larger sample for larger datasets
        Downcasts integers, floats and converts most objects to categories
        Usful for saving memory on large datasets
        Does not parse dates. Will do that separately
        """
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
        """
        standardizes headers to loweracse
        removes preceding and trailing spaces
        replaces remaining spaces with underscores
        """
        try:
            self._df.columns = self._df.columns.str.strip().str.lower().str.replace(' ', '_')
            return self  # Return self for chaining
        except Exception as e:
            raise ValueError(f"Failed to standardize headers: {e}")

    def standard_headers_upper(self):
        """
        standardizes headers to loweracse
        removes preceding and trailing spaces
        replaces remaining spaces with underscores
        """
        try:
            self._df.columns = self._df.columns.str.strip().str.upper().str.replace(' ', '_')
            return self  # Return self for chaining
        except Exception as e:
            raise ValueError(f"Failed to standardize headers: {e}")

    def auto_date_table(start_date,end_date=None, periods=None,eval_date=None, freq='D'):
        """
        Create a date table starting from `start_date` with a specified number of periods and frequency.
        Also adds boolean filters relative to the date the dataframe is created

        start_date = Begining Date
        end_date   = final_date
        eval_date  = optional argument to determine what the present day is. Pass string in 'YYYY-MM-DD' format. 
                     defaults to the date the dataframe is built
        """
         # Create a date range starting from 'start_date' with 'periods' number of periods
         
        date_range = pd.date_range(start=start_date,end=end_date, periods=periods, freq=freq)

        # Create a DataFrame from the date range
        date_table = pd.DataFrame({'date': date_range})

        # Add additional date-related columns
        date_table['year'] = date_table['date'].dt.year
        date_table['month'] = date_table['date'].dt.month
        date_table['quarter'] = date_table['date'].dt.quarter
        date_table['week_number'] = date_table['date'].dt.isocalendar().week
        date_table['day'] = date_table['date'].dt.day
        date_table['day_of_year'] = date_table['date'].dt.day_of_year
        
        date_table['day_of_week'] = date_table['date'].dt.day_name()
        date_table['is_weekend'] = date_table['day_of_week'].isin(['Saturday', 'Sunday'])
        
        # Start dates (use dt.to_period and then convert back to timestamp)
        date_table['year_start_date'] = date_table['date'].dt.to_period('Y').dt.start_time
        date_table['quarter_start_date'] = date_table['date'].dt.to_period('Q').dt.start_time
        date_table['month_start_date'] = date_table['date'].dt.to_period('M').dt.start_time
        date_table['week_start_date'] = date_table['date'].dt.to_period('W').dt.start_time
        
        date_table['creation_ts'] = datetime.now()

        if eval_date == None:
            date_table['eval_date'] = date_table['creation_ts']
        else:
            date_table['eval_date'] =pd.to_datetime(eval_date, format='%Y-%m-%d')

        date_table['eval_year'] = date_table['eval_date'].dt.year
        date_table['eval_month'] = date_table['eval_date'].dt.month
        date_table['eval_quarter'] = date_table['eval_date'].dt.quarter
        date_table['eval_week_number'] = date_table['eval_date'].dt.isocalendar().week
        date_table['eval_day'] = date_table['eval_date'].dt.day
        date_table['eval_day_of_year'] = date_table['eval_date'].dt.dayofyear

        date_table['year_start_eval_date'] = date_table['eval_date'].dt.to_period('Y').dt.start_time
        date_table['quarter_start_eval_date'] = date_table['eval_date'].dt.to_period('Q').dt.start_time
        date_table['month_start_eval_date'] = date_table['eval_date'].dt.to_period('M').dt.start_time
        date_table['week_start_eval_date'] = date_table['eval_date'].dt.to_period('W').dt.start_time
        
        date_table['current_year'] = date_table['eval_date'].dt.to_period('Y').dt.start_time
        date_table['current_quarter'] = date_table['eval_date'].dt.to_period('Q').dt.start_time
        date_table['current_month'] = date_table['eval_date'].dt.to_period('M').dt.start_time
        date_table['current_week'] = date_table['eval_date'].dt.to_period('W').dt.start_time
        
        date_table['last_year'] = (date_table['eval_date'] - pd.offsets.YearBegin(2)).dt.to_period('Y').dt.start_time
        date_table['last_quarter'] = (date_table['eval_date'] - pd.offsets.QuarterBegin(1)).dt.to_period('Q').dt.start_time
        date_table['last_month'] = (date_table['eval_date'] - pd.offsets.MonthBegin(1)).dt.to_period('M').dt.start_time
        date_table['last_week'] = (date_table['eval_date'] - pd.offsets.Week(weekday=0)).dt.to_period('W').dt.start_time

        date_table['is_current_year'] = date_table['year_start_date'] == date_table['current_year']
        date_table['is_current_quarter'] = date_table['quarter_start_date'] == date_table['current_quarter']
        date_table['is_current_month'] = date_table['week_start_date'] == date_table['current_month']
        date_table['is_current_week'] = date_table['week_start_date'] == date_table['current_week']

        date_table['is_last_year'] = date_table['year_start_date'] == date_table['last_year']
        date_table['is_last_quarter'] = date_table['quarter_start_date'] == date_table['last_quarter']
        date_table['is_last_month'] = date_table['week_start_date'] == date_table['last_month']
        date_table['is_last_week'] = date_table['week_start_date'] == date_table['last_week']

        date_table['is_complete_year'] = date_table['year_start_date'] < date_table['current_year']       
        date_table['is_complete_quarter'] = date_table['quarter_start_date'] < date_table['current_quarter']
        date_table['is_complete_month'] = date_table['month_start_date'] < date_table['current_month']
        date_table['is_complete_week'] = date_table['week_start_date'] < date_table['current_week']
        
        date_table['is_yoy_ytd_day_incl'] = date_table['day_of_year'] <= date_table['eval_day_of_year']
        date_table['is_yoy_ytd_day_excl'] = date_table['day_of_year'] < date_table['eval_day_of_year']
        date_table['is_yoy_complete_quarter'] = date_table['quarter'] < date_table['eval_quarter']
        date_table['is_yoy_complete_month'] = date_table['month'] < date_table['eval_month']
        date_table['is_yoy_complete_week'] = date_table['week_number'] < date_table['eval_week_number']

        date_table['is_today_or_before'] = date_table['date'] <= date_table['eval_date']
        date_table['is_before_today'] = date_table['date'] < date_table['eval_date']
        
        date_table['is_current_year_ytd_quarter'] = date_table['is_current_year']*date_table['is_complete_quarter'] 
        date_table['is_current_year_ytd_month'] = date_table['is_current_year']*date_table['is_complete_month'] 
        date_table['is_current_year_ytd_week'] = date_table['is_current_year']*date_table['is_complete_week'] 

        date_table['is_last_year_ytd_quarter'] = date_table['is_last_year']*date_table['is_yoy_complete_quarter']
        date_table['is_last_year_ytd_month'] = date_table['is_current_year']*date_table['is_yoy_complete_month']
        date_table['is_last_year_ytd_week'] = date_table['is_current_year']*date_table['is_complete_week'] 

        return date_table
    
