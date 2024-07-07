from pyspark.sql import DataFrame


class Quality:
    
    @staticmethod
    def uniqueness_based_on_columns(df: DataFrame, columns: list[str]):
        """
        Check if the DataFrame has unique combinations of values in the specified columns.

        Args:
            df (DataFrame): The input DataFrame.
            columns (list[str]): A list of column names.

        Raises:
            AssertionError: If the DataFrame has duplicate rows.
        """
        total_rows =df.count()
        total_distinct_rows_based_on_columns = df.select(*columns).distinct().count()
        assert total_rows == total_distinct_rows_based_on_columns, "Data quality check failed. Found duplicate rows."