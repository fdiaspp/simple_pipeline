from data_platform import Operation
import sys
import json


if __name__ == '__main__':
    definitions = json.loads(sys.argv[1])
    
    ops = Operation()
    df = ops.read_json()
    df = ops.apply_incremental_strategy_based_on_datetime(df, start_date='', end_date='', column_name='')
    
    for _df, combination in ops.generate_dataframe_based_on_columns_values(df, columns=[]):
        dataset_name = '.'.join(combination.values())
        ops.write_parquet(_df, path='', partition_by_columns_name=[])