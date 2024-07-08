from pyspark.sql import DataFrame, SparkSession
import os


class Transformation:
    
    @staticmethod
    def derive_date_fields(df: DataFrame, 
                           year: bool = True, 
                           month: bool = True, 
                           day: bool = True, 
                           hour: bool = False):
        """
        Derives additional date fields from the 'date' column in the DataFrame.

        Args:
            df: The input DataFrame.
            year: If True, extract the year from the 'date' column.
            month: If True, extract the month from the 'date' column.
            day: If True, extract the day from the 'date' column.
            hour: If True, extract the hour from the 'date' column.

        Returns:
            DataFrame: The DataFrame with additional date fields derived based on the 'date' column.
        """
        if year:
            df = df.withColumn('year', df['date'].substr(1, 4))
        if month:
            df = df.withColumn('month', df['date'].substr(6, 2))
        if day:
            df = df.withColumn('day', df['date'].substr(9, 2))
        if hour:
            df = df.withColumn('hour', df['date'].substr(12, 2))
        return df

    @staticmethod
    def execute_sql(df: DataFrame, 
                    sql: str, 
                    tmp_view_name: str = 'tmp') -> DataFrame:
        """
        Executes a SQL query on a DataFrame using a temporary view.

        Args:
            df (DataFrame): The input DataFrame.
            spark_session (SparkSession): The SparkSession to use for executing the SQL query.
            sql (str): The SQL query to execute.
            tmp_view_name (str, optional): The name of the temporary view to create for the DataFrame. Defaults to 'tmp'.

        Returns:
            DataFrame: The result of executing the SQL query on the DataFrame.
        """
        spark_session = (SparkSession
                        .builder
                        .appName(os.environ['SIMPLE_PIPELINE_PYSPAK_APP_NAME'])
                        .getOrCreate())
        
        df.createTempView(tmp_view_name)
        df = spark_session.sql(sql)
        return df