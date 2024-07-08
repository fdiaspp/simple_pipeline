from core import Operation, Cleansing, Quality, Transformation
from typing import Union, Type
from pyspark.sql import DataFrame
import os
import json
import sys


os.environ['SIMPLE_PIPELINE_PYSPAK_APP_NAME'] = 'app'

def resolve_operation(df: DataFrame, cls: Union[Type[Cleansing], Type[Quality]], operations: dict ):
    for operation in operations:
        df = getattr(cls, operation)(df, **operations[operation])
    return df


if __name__ == '__main__':
    print("Params: ", sys.argv[1])
    params = json.loads(sys.argv[1])
    
    ops = Operation()
    input_reader = ops.get_reader(params['input']['type'])
    output_writer = ops.get_writer(params['output']['type'])
    
    df = input_reader(path=params['input']['path'])
    
    operation_mapper = {
        'cleansing': Cleansing,
        'quality': Quality,
        'transformation': Transformation
    } 
    for key in params:
        if key in operation_mapper:
            df = resolve_operation(df=df, cls=operation_mapper[key], operations=params[key])
    
    output_writer(df, 
                  path=params['output']['path'], 
                  partition_by=params['output'].get('partition_by', []),
                  mode=params['output'].get('mode', 'error'),
                  shard_based_on_columns=params['output'].get('shard_based_on_columns'))