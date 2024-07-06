from pyspark.sql import SparkSession, DataFrame


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