from core import Operation, Cleansing, Quality
from typing import Union, Type
from pyspark.sql import DataFrame
import json
import sys


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
 
    df = resolve_operation(df=df, cls=Cleansing, operations=params.get('cleansing', {}))
    df = resolve_operation(df=df, cls=Quality, operations=params.get('quality', {}))     
    
    output_writer(df, 
                  path=params['output']['path'], 
                  partition_by=params['output'].get('partition_by', []),
                  mode=params['output'].get('mode', 'error'))