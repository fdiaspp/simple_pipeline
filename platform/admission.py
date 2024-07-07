from core import Operation, Cleansing
import json
import sys

if __name__ == '__main__':
    definitions = json.loads(sys.argv[1])
    
    print("PRRIIIIINTTT", definitions['hello'])
    # ops = Operation()
    # df = ops.read_json()
    # df = ops.apply_incremental_strategy_based_on_datetime(df, start_date='', end_date='', column_name='')
    
    # df = Cleansing.keep_most_recent_register_based_on_column(df, column_name='', identity_column_name='')
    # ops.write_parquet(df, path='', partition_by_columns_name=[])