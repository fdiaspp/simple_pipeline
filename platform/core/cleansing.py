from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f


class Cleansing:
    
    @staticmethod
    def keep_most_recent_register_based_on_column(df: DataFrame, column_name: str, identity_column_name: str) -> DataFrame:
        """
        Keep only the most recent register based on a specified column for each identity.

        Args:
            df (DataFrame): The input DataFrame.
            column_name (str): The name of the column to order by.
            identity_column_name (str): The name of the column to partition by.

        Returns:
            DataFrame: The filtered DataFrame with only the most recent register for each identity.
        """
        window = Window.partitionBy(identity_column_name).orderBy(f.desc(column_name))
        return df.withColumn('row_number', f.row_number().over(window)).filter(f.col('row_number') == 1).drop('row_number')