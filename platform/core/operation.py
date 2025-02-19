from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from typing import Tuple, List, Iterator, Callable
from datetime import datetime
import os


class Operation:

    def __init__(self) -> None:
        self.spark: SparkSession = (SparkSession
                                    .builder
                                    .appName(os.environ['SIMPLE_PIPELINE_PYSPAK_APP_NAME'])
                                    .getOrCreate())
    
    def get_reader(self, type: str) -> Callable:
        """
        Returns a reader function based on the specified type.

        Args:
            type (str): The type of reader to retrieve.

        Returns:
            Callable: The reader function.

        Raises:
            KeyError: If the specified type is not supported.
        """
        mapper = {
            'json': self.read_json,
            'parquet': self.read_parquet}
        return mapper[type]
    
    def get_writer(self, type: str) -> Callable: 
        """
        Returns a writer function based on the specified type.

        Args:
            type (str): The type of writer to retrieve.

        Returns:
            Callable: The writer function.

        Raises:
            KeyError: If the specified type is not found in the mapper.
        """
        mapper = {'parquet': self.write_parquet}
        return mapper[type]
            
        
    def read_json(self, path: str) -> DataFrame:
        """
        Reads a JSON file from the given path and returns a DataFrame.

        Args:
            path (str): The path to the JSON file.

        Returns:
            DataFrame: The DataFrame read from the JSON file.
        """
        return self.spark.read.json(path)
    
    def read_parquet(self, path: str) -> DataFrame:
        """
        Reads a parquet file from the given path and returns a DataFrame.

        Args:
            path (str): The path to the parquet file.

        Returns:
            DataFrame: The DataFrame read from the parquet file.
        """
        return self.spark.read.parquet(path)
    
    def write_parquet(self, 
                      df: DataFrame, 
                      path: str, 
                      partition_by: List[str] = None,
                      mode: str = 'error',
                      shard_based_on_columns: List[str] = None) -> None:
        """
        Writes a DataFrame to a Parquet file.

        Args:
            df (DataFrame): The DataFrame to write.
            path (str): The path to the Parquet file.
            partition_by (List[str], optional): The list of column names to partition the Parquet file by. Defaults to None.
            mode (str, optional): The write mode. Possible values are 'overwrite', 'append', 'error', 'ignore'. Defaults to 'error'.
            shard_based_on_columns (List[str], optional): The list of column names to shard the Parquet file based on. Defaults to None.

        Returns:
            None: This function does not return anything.

        If `shard_based_on_columns` is provided, the DataFrame is sharded based on the values of the specified columns. 
        Each shard is written to a separate Parquet file under the specified path, with the shard name derived from the values of the specified columns. 
        If `shard_based_on_columns` is not provided, the DataFrame is written to a single Parquet file under the specified path.
        """
        if partition_by:
            for column in partition_by:
                if dict(df.dtypes)[column] == 'int':
                    df = df.withColumn(column + '_2', f.format_string('%02d', df[column])).drop(column).withColumnRenamed(column + '_2', column)

        if shard_based_on_columns:
            for partial_df, row in self.generate_dataframe_based_on_columns_values(df, shard_based_on_columns):
                (partial_df
                 .write
                 .mode(mode)
                 .partitionBy(partition_by if partition_by else [])
                 .parquet(path + f"/{'.'.join(list(row.values()))}"))
        else:
            (df
            .write
            .mode(mode)
            .partitionBy(partition_by if partition_by else [])
            .parquet(path))

    def generate_dataframe_based_on_columns_values(self, df: DataFrame, columns: list[str]) -> Iterator[Tuple[DataFrame, dict]]:
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
            
            yield partial_df, row.asDict()
            
    def apply_incremental_strategy_based_on_datetime(self, df: DataFrame, start_date: datetime, end_date: datetime, column_name: str) -> DataFrame:
        """
        Apply an incremental strategy based on a given datetime range.

        Args:
            df (DataFrame): The input DataFrame.
            start_date (datetime): The start date of the datetime range.
            end_date (datetime): The end date of the datetime range.
            column_name (str): The name of the column to filter.

        Returns:
            DataFrame: The filtered DataFrame.
        """
        filter = f"{column_name} >= '{start_date}' and {column_name} < '{end_date}'"
        return df.where(filter)