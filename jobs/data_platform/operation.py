from pyspark.sql import SparkSession, DataFrame
from typing import Iterable

class Operation:
    
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark = spark_session
        
    def read_json(self, path: str) -> DataFrame:
        """
        Reads a JSON file from the given path and returns a DataFrame.

        Args:
            path (str): The path to the JSON file.

        Returns:
            DataFrame: The DataFrame read from the JSON file.
        """
        return self.spark.read.json(path)
    
    def write_parquet(self, df: DataFrame, path: str) -> None:
        """
        Writes a DataFrame to a Parquet file.

        Args:
            df (DataFrame): The DataFrame to write.
            path (str): The path to the Parquet file.

        Returns:
            None: This function does not return anything.
        """
        df.write.parquet(path)
        
    def generate_dataframe_based_on_columns_values(self, df: DataFrame, columns: list[str]) -> Iterable[DataFrame]:
        """
        Generates a sequence of DataFrames based on the unique combinations of values in the specified columns.

        Args:
            df (DataFrame): The input DataFrame.
            columns (list[str]): A list of column names.

        Yields:
            DataFrame: A DataFrame containing the rows that match the specified column values.
        """
        
        combinations = df.select(*columns).distinct().collect()
        
        for row in combinations:
            filter = " and ".join([ f"{col} = '{getattr(row, col)}'" for col in columns])
            partial_df = df.where(filter)
            
            yield partial_df